/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rsm.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.s3.keys.LastModifiedReverseIndexKey;
import org.apache.kafka.rsm.s3.keys.LogFileKey;
import org.apache.kafka.rsm.s3.keys.MarkerKey;
import org.apache.kafka.rsm.s3.keys.OffsetIndexFileKey;
import org.apache.kafka.rsm.s3.keys.RemoteLogIndexFileKey;
import org.apache.kafka.rsm.s3.keys.TimeIndexFileKey;

import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.log.remote.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

// TODO document the implementation:
//  - consistency model of S3
//  - RDI format
//  - segments on the logical and physical level
//  - read without marker will be successful (RDI)
//  - markers
//  - delete - first delete marker, all others - last write marker
//  - last offset reverse index and time clean-up
//  - migration (moving files and everything should work)
//  - reliance on the lexicographical order of list
//  - leader epoch and their priority
//  - marker, reverse index, data file upload and delete order.
public class S3RemoteStorageManager implements RemoteStorageManager {

    // TODO bucket migration test

    // TODO handle the situation with several leader epochs, test it

    // TODO handle concurrent cleaning and deletion (leader epochs)

    // for testing
    private Integer maxKeys = null;
    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private String bucket;
    private AmazonS3 s3Client;
    private TransferManager transferManager;

    private int indexIntervalBytes;

    private final ConcurrentHashMap<TopicPartition, TopicPartitionRemoteStorageManager> topicPartitionManagers = new ConcurrentHashMap<>();

    public S3RemoteStorageManager() {
    }

    // for testing
    S3RemoteStorageManager(AwsClientBuilder.EndpointConfiguration endpointConfiguration, Integer maxKeys) {
        this.endpointConfiguration = endpointConfiguration;
        this.maxKeys = maxKeys;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(configs);
        this.bucket = config.getS3BucketName();

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (this.endpointConfiguration == null) {
            s3ClientBuilder = s3ClientBuilder.withRegion(config.getS3Region());
        } else {
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }

        // It's fine to pass null in here.
        s3ClientBuilder.setCredentials(config.getAwsCredentialsProvider());

        s3Client = s3ClientBuilder.build();
        transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();

        this.indexIntervalBytes = config.getIndexIntervalBytes();
    }

    @Override
    public long earliestLogOffset(TopicPartition tp) throws IOException {
        return topicPartitionManager(tp).earliestLogOffset();
    }

    @Override
    public List<RemoteLogIndexEntry> copyLogSegment(TopicPartition topicPartition,
                                                    LogSegment logSegment,
                                                    int leaderEpoch) throws IOException {
        return topicPartitionManager(topicPartition).copyLogSegment(logSegment, leaderEpoch);
    }

    @Override
    public void cancelCopyingLogSegment(TopicPartition topicPartition) {
        topicPartitionManager(topicPartition).cancelCopyingLogSegment();
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition) throws IOException {
        return listRemoteSegments(topicPartition, 0);
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition, long minBaseOffset) throws IOException {
        return topicPartitionManager(topicPartition).listRemoteSegments(minBaseOffset);
    }

    @Override
    public List<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        return topicPartitionManager(remoteLogSegment.topicPartition()).getRemoteLogIndexEntries(remoteLogSegment);
    }

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        // Not needed
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean deleteTopicPartition(TopicPartition topicPartition) {
        return topicPartitionManager(topicPartition).deleteTopicPartition();
    }

    @Override
    public long cleanupLogUntil(TopicPartition topicPartition, long cleanUpTillMs) throws IOException {
        return topicPartitionManager(topicPartition).cleanupLogUntil(cleanUpTillMs);
    }

    @Override
    public Records read(TopicPartition topicPartition,
                        RemoteLogIndexEntry remoteLogIndexEntry,
                        int maxBytes,
                        long startOffset,
                        boolean minOneMessage) throws IOException {
        return topicPartitionManager(topicPartition).read(remoteLogIndexEntry, maxBytes, startOffset, minOneMessage);
    }

    @Override
    public void close() {
        topicPartitionManagers.values().forEach(TopicPartitionRemoteStorageManager::close);
        if (transferManager != null) {
            transferManager.shutdownNow(true);
        }
    }

    private TopicPartitionRemoteStorageManager topicPartitionManager(TopicPartition topicPartition) {
        return topicPartitionManagers.computeIfAbsent(topicPartition,
            (tp) -> new TopicPartitionRemoteStorageManager(
                tp, bucket, s3Client, transferManager, maxKeys, indexIntervalBytes
            )
        );
    }
}

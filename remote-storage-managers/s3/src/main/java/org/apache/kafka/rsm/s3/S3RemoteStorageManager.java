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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.rsm.s3.keys.LogFileKey;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.log.remote.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

// TODO figure out and document multi file consistency!

// TODO document the implementation:
//  - consistency model of S3
//  - RDI format
//  - read without marker will be successful (RDI)
//  - last offset reverse index and time clean-up
//  - migration (moving files and everything should work)
//  - leader epoch and their priority
//  - delete - first delete marker, all others - last write marker
//  - marker, reverse index, data file upload and delete order.

/**
 * {@link RemoteStorageManager} implementation backed by AWS S3.
 *
 * <p>This implementation relies heavily on S3's ability to list objects with a prefix and its guarantee to
 * list objects in the lexicographical order, also optionally returning only keys that lexicographically come
 * after a specified key (even non-existing).
 *
 * <p>The implementation also uses some measures to overcome an S3's limitation,
 * the lack of atomic operations on groups of files.
 *
 * <p>Files on remote storage might be deleted or overwritten with the same content,
 * but never overwritten with different content.
 *
 * <p>For each log segment, the storage manager uploads a set of files to the remote tier:
 * <ul>
 *     <li>segment files created by Kafka: the log segment file itself, the offset and time indexes;</li>
 *     <li>the remote log index file;</li>
 *     <li>the last modified reverse index;</li>
 *     <li>the marker file.</li>
 * </ul>
 *
 * <p>The layout is the following:
 * <pre>
 *   {bucket} / {topic}-{partition} /
 *     log /
 *       {base-offset}-{last-offset}-le{leader-epoch}
 *     index /
 *       {base-offset}-{last-offset}-le{leader-epoch}
 *     time-index /
 *       {base-offset}-{last-offset}-le{leader-epoch}
 *     remote-log-index /
 *       {base-offset}-{last-offset}-le{leader-epoch}
 *     last-modified-reverse-index /
 *       {last-modified-ms}-{base-offset}-{last-offset}-le{leader-epoch}
 *     marker /
 *       {base-offset}-{last-offset}-le{leader-epoch}
 * </pre>
 *
 * <p>Each file is uniquely identified by three values:
 * <ul>
 *     <li>the base offset of the segment;</li>
 *     <li>the last offset in the segment;</li>
 *     <li>the epoch of the leader which uploaded the file.</li>
 * </ul>
 *
 * <p>The remote log index file stores {@link RemoteLogIndexEntry}s.
 *
 * <p>S3 doesn't provide an ordered index for objects by an associated timestamp.
 * However, this is needed to find the oldest segments by timestamp for
 * {@link S3RemoteStorageManager#cleanupLogUntil(TopicPartition, long)} operation.
 * The last modified reverse index is a timestamp index created by the storage manager itself.
 * Each key is prefixed by the timestamp when the segment file was last modified before uploading
 * ({@link LogSegment#lastModified()}), the rest is the usual key suffix for a segment's files.
 * The files themselves are empty.
 *
 * <p>When the cleanup operation is executed, the last modified reverse index entries' list is requested from S3.
 * Since S3 lists files in the lexicographical order, the implementation reads key names until
 * {@code {last-modified-ms}} component is less or equal to the specified timestamp.
 * This effectively gives the list of the segments that were modified up until the specified timestamp.
 *
 * <p>The last modified reverse index entries are uploaded strictly before other files and deleted strictly after them
 * to keep the whole segment's file set visible for cleaning even in case of partial uploading.
 *
 * <p>The markers are used for the indication that a segment has been completely uploaded and is available for use.
 * This is needed to overcome an S3's limitation of the lack of atomic operations on groups of files.
 * Markers are uploaded strictly after all other files and deleted strictly before them.
 * Without a marker, the segment is invisible.
 *
 */
public class S3RemoteStorageManager implements RemoteStorageManager {

    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    // TODO eventual consistency caveat (https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel): GET before PUT

    // TODO do we need index and time index?

    // TODO handle the situation with several leader epochs, test it

    // TODO handle concurrent cleaning and deletion (leader epochs)

    // TODO garbage collection in S3 (orphan files, etc)

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
    public Records read(RemoteLogIndexEntry remoteLogIndexEntry,
                        int maxBytes,
                        long startOffset,
                        boolean minOneMessage) throws IOException {
        RdiParsed rdiParsed = new RdiParsed(remoteLogIndexEntry.rdi());
        TopicPartition topicPartition = LogFileKey.getTopicPartition(rdiParsed.getS3Key());
        return topicPartitionManager(topicPartition)
            .read(remoteLogIndexEntry, maxBytes, startOffset, minOneMessage);
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

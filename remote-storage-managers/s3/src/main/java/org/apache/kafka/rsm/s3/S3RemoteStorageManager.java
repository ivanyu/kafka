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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import kafka.log.Log;
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
public class S3RemoteStorageManager implements RemoteStorageManager {

    // TODO log
    // TODO leader epoch
    // TODO migration test

    private static final String MARKER_FILES_DIRECTORY = "marker";
    private static final String LOG_FILES_DIRECTORY = "log";
    private static final String OFFSET_INDEX_FILES_DIRECTORY = "index";
    private static final String TIMESTAMP_INDEX_FILES_DIRECTORY = "time-index";
    private static final String REMOTE_LOG_INDEX_FILES_DIRECTORY = "remote-log-index";
    private static final String LARGEST_TIMESTAMP_REVERSE_INDEX_FILES_DIRECTORY = "largest-timestamp-reverse-index";

    private static final Pattern REMOTE_SEGMENT_NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})");

    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    // for testing
    private Integer maxKeys = null;
    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private String bucket;
    private AmazonS3 s3Client;
    private TransferManager transferManager;

    private int indexIntervalBytes;

    static final String RDI_POSITION_SEPARATOR = "#";
    private static final Pattern RDI_PATTERN = Pattern.compile("(.*)" + RDI_POSITION_SEPARATOR + "(\\d+)");

    private final ConcurrentHashMap<TopicPartition, TopicPartitionCopying> ongoingCopyings = new ConcurrentHashMap<>();

    private final ThreadLocal<BufferSupplier> decompressionBufferSupplier = ThreadLocal.withInitial(BufferSupplier::create);

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
        String fileS3KeyPrefix = fileS3KeyPrefix(tp, MARKER_FILES_DIRECTORY) + "/";
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(fileS3KeyPrefix)
            .withMaxKeys(maxKeys); // it's ok to pass null here
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    assert objectSummary.getKey().startsWith(fileS3KeyPrefix);
                    String keyWithoutPrefix = objectSummary.getKey().substring(fileS3KeyPrefix.length());
                    Matcher m = REMOTE_SEGMENT_NAME_PATTERN.matcher(keyWithoutPrefix);
                    if (m.matches()) {
                        return Long.parseLong(m.group(1));
                    } else {
                        log.warn("File {} has incorrect name format, skipping", objectSummary.getKey());
                    }
                }

                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());
        } catch (SdkClientException e) {
            throw new KafkaException("Error finding earliest offset in " + tp, e);
        }

        return -1L;
    }

    @Override
    public List<RemoteLogIndexEntry> copyLogSegment(TopicPartition topicPartition,
                                                    LogSegment logSegment) throws IOException {
        // TODO concurrent upload among several brokers - document, etc
        // TODO don't copy if marker exists

        // There are no concurrent calls per topic-partition.
        if (ongoingCopyings.containsKey(topicPartition)) {
            throw new IllegalStateException("Already ongoing copying for " + topicPartition);
        }

        TopicPartitionCopying copying = new TopicPartitionCopying(topicPartition, logSegment, bucket, transferManager, indexIntervalBytes);
        ongoingCopyings.put(topicPartition, copying);
        try {
            return copying.copy();
        } finally {
            ongoingCopyings.remove(topicPartition);
        }
    }

    @Override
    public void cancelCopyingLogSegment(TopicPartition topicPartition) {
        TopicPartitionCopying copying = ongoingCopyings.remove(topicPartition);
        if (copying != null) {
            copying.cancel();
        }
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition) throws IOException {
        return listRemoteSegments(topicPartition, 0);
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition, long minBaseOffset) throws IOException {
        List<RemoteLogSegmentInfo> result = new ArrayList<>();

        String fileS3KeyPrefix = fileS3KeyPrefix(topicPartition, MARKER_FILES_DIRECTORY) + "/";
        String startAfterKey = fileS3KeyBaseOffset(topicPartition, MARKER_FILES_DIRECTORY, minBaseOffset);
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(fileS3KeyPrefix)
            .withMaxKeys(maxKeys) // it's ok to pass null here
            .withStartAfter(startAfterKey);
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    assert objectSummary.getKey().startsWith(fileS3KeyPrefix);
                    String keyWithoutPrefix = objectSummary.getKey().substring(fileS3KeyPrefix.length());
                    Matcher m = REMOTE_SEGMENT_NAME_PATTERN.matcher(keyWithoutPrefix);
                    if (m.matches()) {
                        long baseOffset = Long.parseLong(m.group(1));

                        if (baseOffset < minBaseOffset) {
                            log.warn("Requested files starting from key {}, but got {}", startAfterKey, objectSummary.getKey());
                        }
                        assert baseOffset >= minBaseOffset;

                        long endOffset = Long.parseLong(m.group(2));
                        RemoteLogSegmentInfo segment = new RemoteLogSegmentInfo(baseOffset, endOffset, topicPartition, Collections.emptyMap());
                        result.add(segment);
                    } else {
                        log.warn("File {} has incorrect name format, skipping", objectSummary.getKey());
                    }
                }

                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());
        } catch (SdkClientException e) {
            throw new KafkaException("Error listing remote segments in " + topicPartition + " with min base offset " + minBaseOffset, e);
        }

        // No need to explicitly sort the result on our side.
        // According to the AWS documentation (https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html),
        // "Amazon S3 lists objects in UTF-8 character encoding in lexicographical order."
        // Of course, it's safer just to sort. However, we rely on this ordering pretty heavily in other parts
        // and if it's broken in AWS for some reason the whole implementation is broken anyway.
        return result;
    }

    @Override
    public List<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        TopicPartition topicPartition = remoteLogSegment.topicPartition();
        long baseOffset = remoteLogSegment.baseOffset();
        long lastOffset = remoteLogSegment.endOffset();

        if (!checkMarker(topicPartition, baseOffset, lastOffset)) {
            throw new KafkaException("Marker for " + remoteLogSegment + " doesn't exist");
        }

        String remoteLogIndexFileKey = remoteLogIndexFileKey(topicPartition, baseOffset, lastOffset);
        try (S3Object s3Object = s3Client.getObject(bucket, remoteLogIndexFileKey);
             S3ObjectInputStream is = s3Object.getObjectContent()) {

            return JavaConverters.seqAsJavaList(RemoteLogIndexEntry.readAll(is));
        } catch (SdkClientException e) {
            throw new KafkaException("Error reading remote log index file " + remoteLogIndexFileKey, e);
        }
    }

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        // Not needed
        throw new RuntimeException("not implemented");
    }

    private boolean checkMarker(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        String markerKey = markerFileKey(topicPartition, baseOffset, lastOffset);
        try {
            return s3Client.doesObjectExist(bucket, markerKey);
        } catch (SdkClientException e) {
            throw new KafkaException("Error checking marker file " + markerKey, e);
        }
    }

    @Override
    public boolean deleteTopicPartition(TopicPartition topicPartition) {
        // TODO async deletion (delete marker + background task). Don't forget to handle delete-and-recreate scenario.

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(topicPartitionDirectory(topicPartition))
            .withMaxKeys(maxKeys);  // it's ok to pass null here

        ListObjectsV2Result listObjectsResult;
        try {
            do {
                // TODO validate this from the read-after-delete point of view
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                if (!listObjectsResult.getObjectSummaries().isEmpty()) {
                    String[] keysToDelete = listObjectsResult.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey)
                        .toArray(String[]::new);
                    DeleteObjectsRequest deleteRestOfFilesRequest = new DeleteObjectsRequest(bucket)
                        .withKeys(keysToDelete);
                    s3Client.deleteObjects(deleteRestOfFilesRequest);
                }
            } while (!listObjectsResult.getObjectSummaries().isEmpty());
        } catch (SdkClientException e) {
            throw new KafkaException("Error deleting " + topicPartition, e);
        }

        return false; // TODO what to return?
    }

    @Override
    public long cleanupLogUntil(TopicPartition topicPartition, long cleanUpTillMs) {
        // TODO implement
        // TODO test
        return 0;
    }

    @Override
    public Records read(RemoteLogIndexEntry remoteLogIndexEntry,
                        int maxBytes,
                        long startOffset,
                        boolean minOneMessage) throws IOException {
        if (startOffset > remoteLogIndexEntry.lastOffset()) {
            throw new IllegalArgumentException("startOffset > remoteLogIndexEntry.lastOffset(): "
                + startOffset + " > " + remoteLogIndexEntry.lastOffset());
        }

        ByteBuffer buffer = readBytes(remoteLogIndexEntry);
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        int firstBatchPosition = 0;
        RecordBatch firstBatch = null;
        int bytesCoveredByCompleteBatches = 0;
        
        Iterator<MutableRecordBatch> batchIter = records.batchIterator();
        // Find the first batch to read from.
        while (batchIter.hasNext() && firstBatch == null) {
            RecordBatch batch = batchIter.next();
            if (batch.lastOffset() >= startOffset) {
                firstBatch = batch;
                if (bytesCoveredByCompleteBatches + firstBatch.sizeInBytes() <= maxBytes) {
                    bytesCoveredByCompleteBatches += firstBatch.sizeInBytes();
                }
            } else {
                firstBatchPosition += batch.sizeInBytes();
            }
        }

        // TODO improve implementation and tests once contract is stable

        // Count how many bytes are covered by complete batches until maxBytes is reached.
        while (batchIter.hasNext()) {
            RecordBatch batch = batchIter.next();
            if (bytesCoveredByCompleteBatches + batch.sizeInBytes() > maxBytes) {
                break;
            }
            bytesCoveredByCompleteBatches += batch.sizeInBytes();
        }

        assert bytesCoveredByCompleteBatches <= maxBytes;

        if (bytesCoveredByCompleteBatches == 0) {
            if (minOneMessage && firstBatch != null) {
                Iterator<Record> iterator = firstBatch.iterator();
                if (iterator.hasNext()) {
                    return MemoryRecords.withRecords(
                        firstBatch.magic(),
                        firstBatch.baseOffset(),
                        firstBatch.compressionType(),
                        firstBatch.timestampType(),
                        firstBatch.producerId(),
                        firstBatch.producerEpoch(),
                        firstBatch.baseSequence(),
                        firstBatch.partitionLeaderEpoch(),
                        firstBatch.isTransactional(),
                        new SimpleRecord(iterator.next()));
                } else {
                    return MemoryRecords.EMPTY;
                }
            } else {
                return MemoryRecords.EMPTY;
            }
        } else {
            buffer.position(firstBatchPosition);
            buffer.limit(firstBatchPosition + bytesCoveredByCompleteBatches);
            return MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private ByteBuffer readBytes(RemoteLogIndexEntry remoteLogIndexEntry) throws IOException {
        String rdi = new String(remoteLogIndexEntry.rdi(), StandardCharsets.UTF_8);
        Matcher m = RDI_PATTERN.matcher(rdi);
        if (!m.matches()) {
            throw new IllegalArgumentException("Can't parse RDI: " + rdi);
        }

        String s3Key = m.group(1);
        int position = Integer.parseInt(m.group(2));

        // TODO what if dataLength() is incorrect? (what happens when range request is longer than data?)
        GetObjectRequest getRequest = new GetObjectRequest(bucket, s3Key)
            .withRange(position, remoteLogIndexEntry.dataLength());
        try (S3Object s3Object = s3Client.getObject(getRequest);
             S3ObjectInputStream is = s3Object.getObjectContent()) {
            ByteBuffer buffer = ByteBuffer.allocate(((Long)s3Object.getObjectMetadata().getContentLength()).intValue());
            Utils.readFully(is, buffer);
            buffer.flip();
            return buffer;
        } catch (SdkClientException e) {
            throw new KafkaException("Error reading log file " + s3Key, e);
        }
    }

    @Override
    public void close() {
        // TODO cancel uploads
        // TODO go to closed state
        // TODO abort multipart
        if (transferManager != null) {
            transferManager.shutdownNow(true);
        }
    }

    static String markerFileKey(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return s3KeyForOffsets(topicPartition, MARKER_FILES_DIRECTORY, baseOffset, lastOffset);
    }

    static String logFileKey(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return s3KeyForOffsets(topicPartition, LOG_FILES_DIRECTORY, baseOffset, lastOffset);
    }

    static String offsetIndexFileKey(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return s3KeyForOffsets(topicPartition, OFFSET_INDEX_FILES_DIRECTORY, baseOffset, lastOffset);
    }

    static String timestampIndexFileKey(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return s3KeyForOffsets(topicPartition, TIMESTAMP_INDEX_FILES_DIRECTORY, baseOffset, lastOffset);
    }

    static String remoteLogIndexFileKey(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return s3KeyForOffsets(topicPartition, REMOTE_LOG_INDEX_FILES_DIRECTORY, baseOffset, lastOffset);
    }

    private static String s3KeyForOffsets(TopicPartition topicPartition, String directory, long baseOffset, long lastOffset) {
        return fileS3KeyBaseOffset(topicPartition, directory, baseOffset) + "-" +
            Log.filenamePrefixFromOffset(lastOffset);
    }

    private static String fileS3KeyBaseOffset(TopicPartition topicPartition, String directory, long baseOffset) {
        return fileS3KeyPrefix(topicPartition, directory) + "/" +
            Log.filenamePrefixFromOffset(baseOffset);
    }

    static String largestTimestampReverseIndexFileKey(TopicPartition topicPartition, long largestOffset) {
        return fileS3KeyPrefix(topicPartition, LARGEST_TIMESTAMP_REVERSE_INDEX_FILES_DIRECTORY) + "/" +
            // Offset formatting (20 digits) is fine for timestamps too.
            Log.filenamePrefixFromOffset(largestOffset);
    }

    private static String fileS3KeyPrefix(TopicPartition topicPartition, String directory) {
        return topicPartitionDirectory(topicPartition) + directory;
    }

    private static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString() + "/";
    }
}

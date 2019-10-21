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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.s3.keys.LastModifiedReverseIndexKey;
import org.apache.kafka.rsm.s3.keys.LogFileKey;
import org.apache.kafka.rsm.s3.keys.OffsetIndexFileKey;
import org.apache.kafka.rsm.s3.keys.RemoteLogIndexFileKey;
import org.apache.kafka.rsm.s3.keys.TimeIndexFileKey;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

class TopicPartitionRemoteStorageManager {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionRemoteStorageManager.class);
    private final String logPrefix;

    private final TopicPartition topicPartition;

    private final String bucket;
    private final AmazonS3 s3Client;
    private final TransferManager transferManager;

    private final Integer maxKeys;
    private final int indexIntervalBytes;

    private final AtomicReference<TopicPartitionUploading> ongoingUploadings = new AtomicReference<>();

    TopicPartitionRemoteStorageManager(TopicPartition topicPartition,
                                       String bucket,
                                       AmazonS3 s3Client,
                                       TransferManager transferManager,
                                       Integer maxKeys,
                                       int indexIntervalBytes) {
        this.logPrefix = topicPartition.toString();
        this.topicPartition = topicPartition;
        this.bucket = bucket;
        this.s3Client = s3Client;
        this.transferManager = transferManager;
        this.maxKeys = maxKeys;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    long earliestLogOffset() throws IOException {
        String directoryPrefix = LogFileKey.directoryPrefix(topicPartition);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(directoryPrefix);
        log.debug("[{}] Listing objects on S3 with request {}", logPrefix, listObjectsRequest);

        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("[{}] Received object list from S3: {}", logPrefix, listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    try {
                        SegmentInfo segmentInfo = SegmentInfo.parse(key.substring(directoryPrefix.length()));
                        return segmentInfo.baseOffset();
                    } catch (IllegalArgumentException e) {
                        log.warn("[{}] File {} has incorrect name format, skipping", logPrefix, key);
                    }
                }

                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());
        } catch (SdkClientException e) {
            throw new KafkaException("Error finding earliest offset in " + topicPartition, e);
        }

        return -1L;
    }

    List<RemoteLogIndexEntry> copyLogSegment(LogSegment logSegment,
                                             int leaderEpoch) throws IOException {

        // TODO concurrent upload among several brokers - document, etc
        // TODO use e-tag to not overwrite with the same content (fix the doc also)

        // There are no concurrent calls per topic-partition.
        if (ongoingUploadings.get() != null) {
            throw new IllegalStateException("Already ongoing uploading for " + topicPartition);
        }

        if (logSegment.size() == 0) {
            throw new AssertionError("Log segment size must be > 0");
        }

        TopicPartitionUploading uploading = new TopicPartitionUploading(
            topicPartition, leaderEpoch, logSegment, bucket, transferManager, indexIntervalBytes);
        ongoingUploadings.set(uploading);
        try {
            return uploading.upload();
        } finally {
            ongoingUploadings.set(null);
        }
    }

    void cancelUploadingLogSegment() {
        TopicPartitionUploading uploading = ongoingUploadings.getAndSet(null);
        if (uploading != null) {
            uploading.cancel();
        }
    }

    List<RemoteLogSegmentInfo> listRemoteSegments(long minBaseOffset) throws IOException {
        String directoryPrefix = LogFileKey.directoryPrefix(topicPartition);
        String startAfterKey = LogFileKey.baseOffsetPrefix(topicPartition, minBaseOffset);
        ListObjectsV2Request listObjectsRequest =
            createListObjectsRequest(directoryPrefix)
                .withStartAfter(startAfterKey);
        log.debug("[{}] Listing objects on S3 with request {}", logPrefix, listObjectsRequest);
        ListObjectsV2Result listObjectsResult;

        List<RemoteLogSegmentInfo> result = new ArrayList<>();
        Set<OffsetPair> seenOffsetPairs = new HashSet<>();
        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("[{}] Received object list from S3: {}", logPrefix, listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    SegmentInfo segmentInfo;
                    try {
                        segmentInfo = SegmentInfo.parse(key.substring(directoryPrefix.length()));
                    } catch (IllegalArgumentException e) {
                        log.warn("[{}] File {} has incorrect name format, skipping", logPrefix, key);
                        continue;
                    }

                    if (segmentInfo.baseOffset() < minBaseOffset) {
                        log.warn("[{}] Requested files starting from key {}, but got {}", logPrefix, startAfterKey, key);
                    }
                    assert segmentInfo.baseOffset() >= minBaseOffset;

                    // One offset pair may appear in different leader epochs, need to prevent duplication.
                    // We rely on lexicographical sorting order, by which earlier leader epochs will appear
                    // and be added the map before later leader epochs.
                    if (!seenOffsetPairs.contains(segmentInfo.offsetPair())) {
                        RemoteLogSegmentInfo segment = new RemoteLogSegmentInfo(
                            segmentInfo.baseOffset(), segmentInfo.lastOffset(), topicPartition, segmentInfo.leaderEpoch(),
                            Collections.emptyMap());
                        result.add(segment);
                        seenOffsetPairs.add(segmentInfo.offsetPair());
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

    List<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        final Optional<SegmentInfo> segmentCoordinates = getSegments(remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset());
        if (!segmentCoordinates.isPresent()) {
            throw new KafkaException("Log file for " + remoteLogSegment + " doesn't exist");
        }

        String remoteLogIndexFileKey = RemoteLogIndexFileKey.key(
            remoteLogSegment.topicPartition(), remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset(),
            segmentCoordinates.get().leaderEpoch());
        log.debug("[{}] Getting remote log index file by key {}", logPrefix, remoteLogIndexFileKey);
        try (S3Object s3Object = s3Client.getObject(bucket, remoteLogIndexFileKey);
             S3ObjectInputStream is = s3Object.getObjectContent()) {

            final Seq<RemoteLogIndexEntry> remoteLogIndexEntrySeq = RemoteLogIndexEntry.readAll(is);
            return JavaConverters.seqAsJavaList(remoteLogIndexEntrySeq);
        } catch (SdkClientException e) {
            throw new KafkaException("Error reading remote log index file " + remoteLogIndexFileKey, e);
        }
    }

    /**
     * Returns segment infos for the specified {@code topicPartition}, {@code baseOffset}, and {@code lastOffset}.
     *
     * <p>In case there are several segments for them (different leader epochs), the method will return the one with
     * the lowest leader epoch.
     * @return a segment info if the segment exists.
     */
    private Optional<SegmentInfo> getSegments(long baseOffset, long lastOffset) {
        final String directoryPrefix = LogFileKey.directoryPrefix(topicPartition);
        String fileS3KeyPrefix = LogFileKey.keyPrefixWithoutLeaderEpochNumber(topicPartition, baseOffset, lastOffset);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(fileS3KeyPrefix);
        log.debug("[{}] Listing objects on S3 with request {}", logPrefix, listObjectsRequest);
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("[{}] Received object list from S3: {}", logPrefix, listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    assert key.startsWith(fileS3KeyPrefix);

                    try {
                        return Optional.of(
                            SegmentInfo.parse(key.substring(directoryPrefix.length()))
                        );
                    } catch (IllegalArgumentException e) {
                        log.warn("[{}] File {} has incorrect name format, skipping", logPrefix, key);
                    }
                }

                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());
        } catch (SdkClientException e) {
            throw new KafkaException("Error getting segment infos in "
                    + topicPartition + " with base offset " + baseOffset + " and last offset " + lastOffset, e);
        }

        return Optional.empty();
    }

    boolean deleteTopicPartition() {
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(topicPartitionDirectory(topicPartition));
        log.debug("[{}] Listing objects on S3 with request {}", logPrefix, listObjectsRequest);

        try {
            List<String> keysToDelete = new ArrayList<>();

            ListObjectsV2Result listObjectsResult;
            do {
                // TODO validate this from the read-after-delete point of view
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("[{}] Received object list from S3: {}", logPrefix, listObjectsResult);

                keysToDelete.addAll(
                    listObjectsResult.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey)
                        .collect(Collectors.toList())
                );
                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());

            deleteKeys(keysToDelete);
        } catch (SdkClientException e) {
            throw new KafkaException("Error deleting " + topicPartition, e);
        }

        return false; // TODO what to return?
    }

    long cleanupLogUntil(long cleanUpTillMs) throws IOException {
        String directoryPrefix = LastModifiedReverseIndexKey.directoryPrefix(topicPartition);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(directoryPrefix);
        log.debug("[{}] Listing objects on S3 with request {}", logPrefix, listObjectsRequest);

        // TODO test interrupted cleanup.

        try {
            // Deletion order:
            // 1. Log file (makes segment unavailable for other operations).
            // 2. Other data files.
            // 3. Last modified reverse index entries (needs to be in the end to enable retrying an interrupted cleanup).

            List<String> logFiles = new ArrayList<>();
            List<String> dataFiles = new ArrayList<>();
            List<String> lastModifiedReverseIndexEntries = new ArrayList<>();

            ListObjectsV2Result listObjectsResult;
            outer:
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("[{}] Received object list from S3: {}", logPrefix, listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    LastModifiedReverseIndexEntry entry;
                    try {
                        entry = LastModifiedReverseIndexEntry.parse(
                            key.substring(directoryPrefix.length())
                        );
                    } catch (IllegalArgumentException e) {
                        log.warn("[{}] File {} has incorrect name format, skipping", logPrefix, key);
                        continue;
                    }

                    log.debug("[{}] Found last modified reverse log entry {}", logPrefix, entry);

                    // Rely on the key listing order.
                    if (entry.lastModifiedMs() > cleanUpTillMs) {
                        log.debug("[{}] Last modified reverse log entry {} is beyond cleanUpTillMs ({}), stopping",
                            logPrefix, entry, cleanUpTillMs);
                        break outer;
                    }

                    logFiles.add(LogFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(OffsetIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(TimeIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(RemoteLogIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    lastModifiedReverseIndexEntries.add(key);
                }
                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());

            log.debug("[{}] Deleting log files: {}", logPrefix, logFiles);
            deleteKeys(logFiles);
            log.debug("[{}] Deleting data files: {}", logPrefix, dataFiles);
            deleteKeys(dataFiles);
            log.debug("[{}] Deleting last modified reverse index entries: {}", logPrefix, lastModifiedReverseIndexEntries);
            deleteKeys(lastModifiedReverseIndexEntries);
        } catch (SdkClientException e) {
            throw new KafkaException("Error cleaning log until " + cleanUpTillMs + " in " + topicPartition, e);
        }

        // for now, return the earliest offset after deletion.
        // TODO confirm it's the right way
        return earliestLogOffset();
    }

    private ListObjectsV2Request createListObjectsRequest(String prefix) {
        return new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(prefix)
            .withMaxKeys(maxKeys); // it's ok to pass null here
    }

    private void deleteKeys(Collection<String> keys) {
        log.debug("[{}] Deleting keys {}", logPrefix, keys);
        List<List<String>> chunks = new ArrayList<>();
        chunks.add(new ArrayList<>());

        for (String key : keys) {
            List<String> lastChunk = chunks.get(chunks.size() - 1);
            if (lastChunk.size() >= 1000) {
                lastChunk = new ArrayList<>();
                chunks.add(lastChunk);
            }
            lastChunk.add(key);
        }

        for (List<String> chunk : chunks) {
            assert chunk.size() <= 1000;

            final String[] chunkArray = chunk.toArray(new String[]{});
            log.debug("[{}] Deleting key chunk {}", logPrefix, Arrays.toString(chunkArray));
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket)
                .withKeys(chunkArray);
            try {
                s3Client.deleteObjects(deleteObjectsRequest);
            } catch (MultiObjectDeleteException e) {
                // On an attempt to delete a non-existent key, real S3 will return no error,
                // but Localstack (used for testing) will. This effectively handles errors that
                // appear only in tests with Localstack.
                // TODO not needed if we switch to integration testing with real S3.
                for (MultiObjectDeleteException.DeleteError error : e.getErrors()) {
                    log.warn("[{}] Error deleting key {}: {} {}",
                        logPrefix, error.getKey(), error.getCode(), error.getMessage());
                }
            } catch (SdkClientException e) {
                throw new KafkaException("Error deleting keys " + Arrays.toString(chunkArray), e);
            }
        }
    }

    Records read(RemoteLogIndexEntry remoteLogIndexEntry,
                 int maxBytes,
                 long startOffset,
                 boolean minOneMessage) throws IOException {

        // TODO what to return in case nothing exists for this offset?
        // TODO test when segments marked for deletion

        if (startOffset > remoteLogIndexEntry.lastOffset()) {
            throw new IllegalArgumentException("startOffset > remoteLogIndexEntry.lastOffset(): "
                + startOffset + " > " + remoteLogIndexEntry.lastOffset());
        }

        ByteBuffer buffer = readBytes(remoteLogIndexEntry);
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        int firstBatchPos = 0;
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
                firstBatchPos += batch.sizeInBytes();
            }
        }
        log.debug("[{}] Position of first batch to read from: {}", logPrefix, firstBatchPos);

        // TODO improve implementation and tests once contract is stable

        // Count how many bytes are covered by complete batches until maxBytes is reached.
        while (batchIter.hasNext()) {
            RecordBatch batch = batchIter.next();
            if (bytesCoveredByCompleteBatches + batch.sizeInBytes() > maxBytes) {
                break;
            }
            bytesCoveredByCompleteBatches += batch.sizeInBytes();
        }
        log.debug("[{}] Bytes covered by complete batches until maxBytes ({}) is reached: {}",
            logPrefix, maxBytes, bytesCoveredByCompleteBatches);

        assert bytesCoveredByCompleteBatches <= maxBytes;

        if (bytesCoveredByCompleteBatches == 0) {
            if (minOneMessage && firstBatch != null) {
                log.debug("[{}] minOneMessage: {}, firstBatch: {}, trying to get first record",
                    logPrefix, minOneMessage, firstBatch);
                Iterator<Record> iterator = firstBatch.iterator();
                if (iterator.hasNext()) {
                    log.debug("[{}] First batch is not empty, returning first record", logPrefix);
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
                    log.debug("[{}] First batch is empty, returning empty records", logPrefix);
                    return MemoryRecords.EMPTY;
                }
            } else {
                log.debug("[{}] minOneMessage: {}, firstBatch: {}, returning empty records",
                    logPrefix, minOneMessage, firstBatch);
                return MemoryRecords.EMPTY;
            }
        } else {
            final int limit = firstBatchPos + bytesCoveredByCompleteBatches;
            log.debug("[{}] Reading records from {} to {}", logPrefix, firstBatchPos, limit);
            buffer.position(firstBatchPos);
            buffer.limit(limit);
            return MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private ByteBuffer readBytes(RemoteLogIndexEntry remoteLogIndexEntry) throws IOException {
        RdiParsed rdiParsed = new RdiParsed(remoteLogIndexEntry.rdi());

        String s3Key = rdiParsed.getS3Key();
        int position = rdiParsed.getPosition();
        log.debug("[{}] S3 key: {}, position: {}", logPrefix, s3Key, position);

        // TODO what if dataLength() is incorrect? (what happens when range request is longer than data?)
        GetObjectRequest getRequest = new GetObjectRequest(bucket, s3Key)
            .withRange(position, position + remoteLogIndexEntry.dataLength());
        log.debug("[{}] Getting data from S3 with request: {}", logPrefix, getRequest);
        try (S3Object s3Object = s3Client.getObject(getRequest);
             S3ObjectInputStream is = s3Object.getObjectContent()) {
            log.debug("[{}] Got S3 object: {}", logPrefix, s3Object);
            int contentLength = (int) s3Object.getObjectMetadata().getContentLength();
            log.debug("[{}] Reading {} bytes", logPrefix, contentLength);
            ByteBuffer buffer = ByteBuffer.allocate(contentLength);
            Utils.readFully(is, buffer);
            buffer.flip();
            return buffer;
        } catch (SdkClientException e) {
            throw new KafkaException("Error reading log file " + s3Key, e);
        }
    }

    void close() {
        cancelUploadingLogSegment();
    }

    private static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString() + "/";
    }
}

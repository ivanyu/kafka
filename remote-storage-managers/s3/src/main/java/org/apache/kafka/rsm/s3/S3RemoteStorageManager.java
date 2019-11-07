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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.log.remote.RemoteStorageManager;
import org.apache.kafka.rsm.s3.files.LastModifiedReverseIndexEntry;
import org.apache.kafka.rsm.s3.files.LogFileS3Key;
import org.apache.kafka.rsm.s3.files.OffsetIndexFileKeyS3Key;
import org.apache.kafka.rsm.s3.files.RemoteLogIndexFileKeyS3Key;
import org.apache.kafka.rsm.s3.files.SegmentInfo;
import org.apache.kafka.rsm.s3.files.TimeIndexFileKeyS3Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * {@link RemoteStorageManager} implementation backed by AWS S3.
 *
 * <p>This implementation relies heavily on S3's ability to list objects with a prefix and its guarantee to
 * list objects in the lexicographical order (from the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html">documentation</a>:
 * "Amazon S3 lists objects in UTF-8 character encoding in lexicographical order."),
 * also optionally returning only keys that lexicographically come after a specified key (even non-existing).
 *
 * <p>S3 has important limitations in the context of this implementation:
 * <ul>
 *     <li>it lacks atomic operations on groups of files, like atomic rename, upload or delete;</li>
 *     <li><a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">
 *         it is eventually consistent</a>.</li>
 * </ul>
 * The implementation tries to mitigate this, which will be described below.
 *
 * <p>Remote Data Identifier (RDI) is presented as {@code {s3-key}#{byte-offset}}.
 *
 * <p>Files on remote storage might be deleted or overwritten with the same content,
 * but never overwritten with different content.
 *
 * <p>For each log segment, the storage manager uploads a set of files to the remote tier:
 * <ul>
 *     <li>segment files created by Kafka: the log segment file itself, the offset and time indexes;</li>
 *     <li>the remote log index file;</li>
 *     <li>the last modified reverse index.</li>
 * </ul>
 *
 * <p>The layout is the following:
 * <pre>
 *   {bucket} / {topic}-{partition} /
 *     log /
 *       {last-offset}-{base-offset}-le{leader-epoch}
 *     index /
 *       {last-offset}-{base-offset}-le{leader-epoch}
 *     time-index /
 *       {last-offset}-{base-offset}-le{leader-epoch}
 *     remote-log-index /
 *       {last-offset}-{base-offset}-le{leader-epoch}
 *     last-modified-reverse-index /
 *       {last-modified-ms}-{last-offset}-{base-offset}-le{leader-epoch}
 * </pre>
 *
 * <p>The reason behind this inverted layout--grouping by file type instead of offset pairs--
 * is that S3 supports prefix scans, so listing all log segments without irrelevant keys
 * is easier this way.
 *
 * <p>Each file is uniquely identified by three values:
 * <ul>
 *     <li>the last offset in the segment;</li>
 *     <li>the base offset of the segment;</li>
 *     <li>the epoch of the leader which uploaded the file.</li>
 * </ul>
 *
 * <p>The reason to put the last offset before the base offset is {@link #listRemoteSegments(TopicPartition, long)}.
 * This method lists segments that contain offsets starting from some specified min offset. It lists segments
 * which last offsets are equals or greater to this min offset.
 * This is naturally done by S3 itself if the last offset is put first.
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
 * <p>To keep the whole segment's file set visible for cleaning even in case of partial uploading
 * (not all files are uploaded), last modified reverse index entries uploading must be confirmed before other files
 * start being uploaded. The same happen on deletion: only when other files' deletion is confirmed,
 * the last modified reverse index file is deleted.
 *
 * <p>The presence of a log file itself serves also as an indication that a segment has been completely uploaded and
 * is available for use. Because of this, they are uploaded strictly after all other files and deleted strictly
 * before all other files (i.e. awaiting S3's confirmations to keep the order).
 * Without the log file, the whole segment is visible only for cleaning up.
 *
 * <p>However, there's a <strong>caveat</strong> regarding operation ordering due to S3's eventual consistency.
 * Even if two operations happen in some strict order from the client point of view, it isn't guaranteed they will be
 * replicated inside S3 in the same order and other clients (even the same one) will see them in the same order.
 * In this implementation, there are no operations in which this can't be overcome by some reasonable retry policy.
 * TODO: double check and confirm this.
 *
 * <p>Sometimes two brokers can think of themselves to be the leader of a partition.
 * It's also not impossible for them to simultaneously try to upload files from a log segment.
 * To break this tie, each file uploaded to the remote tier is suffixed with the leader epoch number.
 * During the read time, the file set with the lower leader epoch number will be used.
 * This will not leave garbage on the remote tier, because
 * {@link S3RemoteStorageManager#cleanupLogUntil(TopicPartition, long)} will collect files to be deleted
 * without taking the leader epoch number into consideration.
 *
 */
public class S3RemoteStorageManager implements RemoteStorageManager {

    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    // TODO do we need index and time index?

    // TODO handle the situation with several leader epochs, test it

    // TODO garbage collection in S3 (orphan files, etc)

    // TODO bucket migration (moving files between buckets)

    // for testing
    private Integer maxKeys = null;
    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private String bucket;
    private AmazonS3 s3Client;
    private TransferManager transferManager;

    private int indexIntervalBytes;

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
        this.bucket = config.s3BucketName();

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (this.endpointConfiguration == null) {
            s3ClientBuilder = s3ClientBuilder.withRegion(config.s3Region());
        } else {
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }

        // It's fine to pass null in here.
        s3ClientBuilder.setCredentials(config.awsCredentialsProvider());

        s3Client = s3ClientBuilder.build();
        transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();

        this.indexIntervalBytes = config.indexIntervalBytes();
    }

    @Override
    public long earliestLogOffset(TopicPartition tp) throws IOException {
        String directoryPrefix = LogFileS3Key.directoryPrefix(tp);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(directoryPrefix);
        log.debug("Listing objects on S3 with request {}", listObjectsRequest);

        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("Received object list from S3: {}", listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    try {
                        SegmentInfo segmentInfo = LogFileS3Key.parse(key);
                        return segmentInfo.baseOffset();
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
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
                                                    LogSegment logSegment,
                                                    int leaderEpoch) throws IOException {
        // TODO concurrent upload among several brokers - document, etc
        // TODO use e-tag to not overwrite with the same content (fix the doc also)

        // There are no concurrent calls per topic-partition.

        if (logSegment.size() == 0) {
            throw new AssertionError("Log segment size must be > 0");
        }

        TopicPartitionUploading uploading = new TopicPartitionUploading(
                topicPartition, leaderEpoch, logSegment, bucket, transferManager, indexIntervalBytes);
        return uploading.upload();
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition) throws IOException {
        return listRemoteSegments(topicPartition, 0);
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition, long minOffset) throws IOException {
        String directoryPrefix = LogFileS3Key.directoryPrefix(topicPartition);
        // The last offset should be less or equal to the min offset.
        String startAfterKey = LogFileS3Key.lastOffsetPrefix(topicPartition, minOffset);
        ListObjectsV2Request listObjectsRequest =
                createListObjectsRequest(directoryPrefix)
                        .withStartAfter(startAfterKey);
        log.debug("Listing objects on S3 with request {}", listObjectsRequest);
        ListObjectsV2Result listObjectsResult;

        List<RemoteLogSegmentInfo> result = new ArrayList<>();
        Set<SegmentInfo.OffsetPair> seenOffsetPairs = new HashSet<>();
        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("Received object list from S3: {}", listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    SegmentInfo segmentInfo;
                    try {
                        segmentInfo = LogFileS3Key.parse(key);
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
                        continue;
                    }

                    if (segmentInfo.lastOffset() < minOffset) {
                        log.warn("Requested files starting from key {}, but got {}", startAfterKey, key);
                    }
                    assert segmentInfo.lastOffset() >= minOffset;

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
            throw new KafkaException("Error listing remote segments in " + topicPartition + " with min offset " + minOffset, e);
        }

        // We need to explicitly sort the result on our side by the base offset,
        // because S3 returns key sorted lexicographically, i.e. by the last offset.
        result.sort(Comparator.comparing(RemoteLogSegmentInfo::baseOffset));
        return result;
    }

    @Override
    public List<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        final Optional<SegmentInfo> segmentCoordinates = getSegments(
                remoteLogSegment.topicPartition(), remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset());
        if (!segmentCoordinates.isPresent()) {
            throw new KafkaException("Log file for " + remoteLogSegment + " doesn't exist");
        }

        String remoteLogIndexFileKey = RemoteLogIndexFileKeyS3Key.key(
                remoteLogSegment.topicPartition(), remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset(),
                segmentCoordinates.get().leaderEpoch());
        log.debug("Getting remote log index file by key {}", remoteLogIndexFileKey);
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
    private Optional<SegmentInfo> getSegments(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        final String directoryPrefix = LogFileS3Key.directoryPrefix(topicPartition);
        String fileS3KeyPrefix = LogFileS3Key.keyPrefixWithoutLeaderEpochNumber(topicPartition, baseOffset, lastOffset);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(fileS3KeyPrefix);
        log.debug("Listing objects on S3 with request {}", listObjectsRequest);
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("Received object list from S3: {}", listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    assert key.startsWith(fileS3KeyPrefix);

                    try {
                        return Optional.of(LogFileS3Key.parse(key));
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
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

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        // Not needed
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean deleteTopicPartition(TopicPartition topicPartition) {
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(topicPartitionDirectory(topicPartition));
        log.debug("Listing objects on S3 with request {}", listObjectsRequest);

        try {
            List<String> keysToDelete = new ArrayList<>();

            ListObjectsV2Result listObjectsResult;
            do {
                // TODO validate this from the read-after-delete point of view
                // https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers
                // might be helpful in the future
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
                log.debug("Received object list from S3: {}", listObjectsResult);

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

    @Override
    public long cleanupLogUntil(TopicPartition topicPartition, long cleanUpTillMs) throws IOException {
        String directoryPrefix = LastModifiedReverseIndexEntry.directoryPrefix(topicPartition);
        ListObjectsV2Request listObjectsRequest = createListObjectsRequest(directoryPrefix);
        log.debug("Listing objects on S3 with request {}", listObjectsRequest);

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
                log.debug("Received object list from S3: {}", listObjectsResult);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    LastModifiedReverseIndexEntry entry;
                    try {
                        entry = LastModifiedReverseIndexEntry.parse(key);
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
                        continue;
                    }

                    log.debug("Found last modified reverse log entry {}", entry);

                    // Rely on the key listing order.
                    if (entry.lastModifiedMs() > cleanUpTillMs) {
                        log.debug("Last modified reverse log entry {} is beyond cleanUpTillMs ({}), stopping",
                                entry, cleanUpTillMs);
                        break outer;
                    }

                    logFiles.add(LogFileS3Key.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(OffsetIndexFileKeyS3Key.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(TimeIndexFileKeyS3Key.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(RemoteLogIndexFileKeyS3Key.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    lastModifiedReverseIndexEntries.add(key);
                }
                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());

            log.debug("Deleting log files: {}", logFiles);
            deleteKeys(logFiles);
            log.debug("Deleting data files: {}", dataFiles);
            deleteKeys(dataFiles);
            log.debug("Deleting last modified reverse index entries: {}", lastModifiedReverseIndexEntries);
            deleteKeys(lastModifiedReverseIndexEntries);
        } catch (SdkClientException e) {
            throw new KafkaException("Error cleaning log until " + cleanUpTillMs + " in " + topicPartition, e);
        }

        // for now, return the earliest offset after deletion.
        // TODO confirm it's the right way
        return earliestLogOffset(topicPartition);
    }

    private ListObjectsV2Request createListObjectsRequest(String prefix) {
        return new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix)
                .withMaxKeys(maxKeys); // it's ok to pass null here
    }

    private void deleteKeys(Collection<String> keys) {
        log.debug("Deleting keys {}", keys);
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
            log.debug("Deleting key chunk {}", Arrays.toString(chunkArray));
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
                    log.warn("Error deleting key {}: {} {}",
                            error.getKey(), error.getCode(), error.getMessage());
                }
            } catch (SdkClientException e) {
                throw new KafkaException("Error deleting keys " + Arrays.toString(chunkArray), e);
            }
        }
    }

    @Override
    public Records read(RemoteLogIndexEntry remoteLogIndexEntry,
                        int maxBytes,
                        long startOffset,
                        boolean minOneMessage) throws IOException {
        // TODO what to return in case nothing exists for this offset?

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
        log.debug("Position of first batch to read from: {}", firstBatchPos);

        // TODO improve implementation and tests once contract is stable

        // Count how many bytes are covered by complete batches until maxBytes is reached.
        while (batchIter.hasNext()) {
            RecordBatch batch = batchIter.next();
            if (bytesCoveredByCompleteBatches + batch.sizeInBytes() > maxBytes) {
                break;
            }
            bytesCoveredByCompleteBatches += batch.sizeInBytes();
        }
        log.debug("Bytes covered by complete batches until maxBytes ({}) is reached: {}",
                maxBytes, bytesCoveredByCompleteBatches);

        assert bytesCoveredByCompleteBatches <= maxBytes;

        if (bytesCoveredByCompleteBatches == 0) {
            if (minOneMessage && firstBatch != null) {
                log.debug("minOneMessage: {}, firstBatch: {}, trying to get first record",
                        minOneMessage, firstBatch);
                Iterator<Record> iterator = firstBatch.iterator();
                if (iterator.hasNext()) {
                    log.debug("First batch is not empty, returning first record");
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
                    log.debug("First batch is empty, returning empty records");
                    return MemoryRecords.EMPTY;
                }
            } else {
                log.debug("minOneMessage: {}, firstBatch: {}, returning empty records",
                        minOneMessage, firstBatch);
                return MemoryRecords.EMPTY;
            }
        } else {
            final int limit = firstBatchPos + bytesCoveredByCompleteBatches;
            log.debug("Reading records from {} to {}", firstBatchPos, limit);
            buffer.position(firstBatchPos);
            buffer.limit(limit);
            return MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private ByteBuffer readBytes(RemoteLogIndexEntry remoteLogIndexEntry) throws IOException {
        S3RDI s3RDI = new S3RDI(remoteLogIndexEntry.rdi());
        String s3Key = s3RDI.s3Key();
        int position = s3RDI.position();
        log.debug("S3 key: {}, position: {}", s3Key, position);

        // TODO what if dataLength() is incorrect? (what happens when range request is longer than data?)
        GetObjectRequest getRequest = new GetObjectRequest(bucket, s3Key)
                .withRange(position, position + remoteLogIndexEntry.dataLength());
        log.debug("Getting data from S3 with request: {}", getRequest);
        try (S3Object s3Object = s3Client.getObject(getRequest);
             S3ObjectInputStream is = s3Object.getObjectContent()) {
            log.debug("Got S3 object: {}", s3Object);
            int contentLength = (int) s3Object.getObjectMetadata().getContentLength();
            log.debug("Reading {} bytes", contentLength);
            ByteBuffer buffer = ByteBuffer.allocate(contentLength);
            Utils.readFully(is, buffer);
            buffer.flip();
            return buffer;
        } catch (SdkClientException e) {
            throw new KafkaException("Error reading log file " + s3Key, e);
        }
    }

    @Override
    public void close() {
    }

    private static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString() + "/";
    }
}

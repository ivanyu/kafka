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

    // TODO log (including other files)
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    // TODO common prefix
    // TODO migration test
    // TODO should path be added to RDI? (because of segment info)

    // TODO handle the situation with several leader epochs, test it

    // TODO handle concurrent cleaning and deletion (leader epochs)

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
        String directoryPrefix = MarkerKey.directoryPrefix(tp);
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(directoryPrefix)
            .withMaxKeys(maxKeys); // it's ok to pass null here
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    try {
                        Marker marker = Marker.parse(key.substring(directoryPrefix.length()));
                        return marker.baseOffset();
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
        // TODO don't copy if marker exists

        // There are no concurrent calls per topic-partition.
        if (ongoingCopyings.containsKey(topicPartition)) {
            throw new IllegalStateException("Already ongoing copying for " + topicPartition);
        }

        TopicPartitionCopying copying = new TopicPartitionCopying(
            leaderEpoch, topicPartition, logSegment, bucket, transferManager, indexIntervalBytes);
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
        String directoryPrefix = MarkerKey.directoryPrefix(topicPartition);
        String startAfterKey = MarkerKey.baseOffsetPrefix(topicPartition, minBaseOffset);
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(directoryPrefix)
            .withMaxKeys(maxKeys) // it's ok to pass null here
            .withStartAfter(startAfterKey);
        ListObjectsV2Result listObjectsResult;

        List<RemoteLogSegmentInfo> result = new ArrayList<>();
        Set<OffsetPair> seenOffsetPairs = new HashSet<>();
        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    Marker marker;
                    try {
                        marker = Marker.parse(key.substring(directoryPrefix.length()));
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
                        continue;
                    }

                    if (marker.baseOffset() < minBaseOffset) {
                        log.warn("Requested files starting from key {}, but got {}", startAfterKey, key);
                    }
                    assert marker.baseOffset() >= minBaseOffset;

                    // One offset pair may appear in different leader epochs, need to prevent duplication.
                    if (!seenOffsetPairs.contains(marker.offsetPair())) {
                        RemoteLogSegmentInfo segment = new RemoteLogSegmentInfo(
                            marker.baseOffset(), marker.lastOffset(), topicPartition, marker.leaderEpoch(),
                            Collections.emptyMap());
                        result.add(segment);
                        seenOffsetPairs.add(marker.offsetPair());
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
        final Optional<Marker> marker = getMarker(
            remoteLogSegment.topicPartition(), remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset());
        if (!marker.isPresent()) {
            throw new KafkaException("Marker for " + remoteLogSegment + " doesn't exist");
        }

        String remoteLogIndexFileKey = RemoteLogIndexFileKey.key(
            remoteLogSegment.topicPartition(), remoteLogSegment.baseOffset(), remoteLogSegment.lastOffset(),
            marker.get().leaderEpoch());
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

    /**
     * Returns a marker for the specified {@code topicPartition}, {@code baseOffset}, and {@code lastOffset}.
     *
     * <p>In case there are several markers for them (different leader epochs), the method will return the one with
     * the lowest leader epoch.
     * @return a marker if it exists.
     */
    private Optional<Marker> getMarker(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        final String directoryPrefix = MarkerKey.directoryPrefix(topicPartition);

        String fileS3KeyPrefix = MarkerKey.keyPrefixWithoutLeaderEpochNumber(topicPartition, baseOffset, lastOffset);
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(fileS3KeyPrefix)
            .withMaxKeys(maxKeys); // it's ok to pass null here
        ListObjectsV2Result listObjectsResult;

        try {
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    assert key.startsWith(fileS3KeyPrefix);

                    try {
                        return Optional.of(
                            Marker.parse(key.substring(directoryPrefix.length()))
                        );
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
                    }
                }

                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());
        } catch (SdkClientException e) {
            throw new KafkaException("Error getting marker in " + topicPartition + " with base offset " + baseOffset + " and last offset " + lastOffset, e);
        }

        return Optional.empty();
    }

    @Override
    public boolean deleteTopicPartition(TopicPartition topicPartition) {
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(topicPartitionDirectory(topicPartition))
            .withMaxKeys(maxKeys);  // it's ok to pass null here

        try {
            List<String> keysToDelete = new ArrayList<>();

            ListObjectsV2Result listObjectsResult;
            do {
                // TODO validate this from the read-after-delete point of view
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);
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
        String directoryPrefix = LastModifiedReverseIndexKey.directoryPrefix(topicPartition);
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(directoryPrefix)
            .withMaxKeys(maxKeys); // it's ok to pass null here

        // TODO test interrupted cleanup.

        try {
            // Deletion order:
            // 1. Markers (makes segment unavailable for other operations).
            // 2. Data.
            // 3. Last modified reverse index entries (needs to be in the end to enable retrying an interrupted cleanup).

            List<String> markers = new ArrayList<>();
            List<String> dataFiles = new ArrayList<>();
            List<String> lastModifiedReverseIndexEntries = new ArrayList<>();

            ListObjectsV2Result listObjectsResult;
            outer:
            do {
                listObjectsResult = s3Client.listObjectsV2(listObjectsRequest);

                for (S3ObjectSummary objectSummary : listObjectsResult.getObjectSummaries()) {
                    final String key = objectSummary.getKey();
                    assert key.startsWith(directoryPrefix);
                    LastModifiedReverseIndexEntry entry;
                    try {
                        entry = LastModifiedReverseIndexEntry.parse(
                            key.substring(directoryPrefix.length())
                        );
                    } catch (IllegalArgumentException e) {
                        log.warn("File {} has incorrect name format, skipping", key);
                        continue;
                    }

                    // Rely on the key listing order.
                    if (entry.lastModifiedMs() > cleanUpTillMs) {
                        break outer;
                    }

                    markers.add(MarkerKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(LogFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(OffsetIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(TimeIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    dataFiles.add(RemoteLogIndexFileKey.key(topicPartition, entry.baseOffset(), entry.lastOffset(), entry.leaderEpoch()));
                    lastModifiedReverseIndexEntries.add(key);
                }
                listObjectsRequest.setContinuationToken(listObjectsResult.getNextContinuationToken());
            } while (listObjectsResult.isTruncated());

            deleteKeys(markers);
            deleteKeys(dataFiles);
            deleteKeys(lastModifiedReverseIndexEntries);
        } catch (SdkClientException e) {
            throw new KafkaException("Error cleaning log until " + cleanUpTillMs + " in " + topicPartition, e);
        }

        // for now, return the earliest offset after deletion.
        // TODO confirm it's the right way
        return earliestLogOffset(topicPartition);
    }

    private void deleteKeys(Collection<String> keys) {
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
            log.trace("Deleting keys {}", Arrays.toString(chunkArray));
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
                    log.warn("Error deleting key {}: {} {}", error.getKey(), error.getCode(), error.getMessage());
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
        // TODO test when segments marked for deletion

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

    private static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString() + "/";
    }
}

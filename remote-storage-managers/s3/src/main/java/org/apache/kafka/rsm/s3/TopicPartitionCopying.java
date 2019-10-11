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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.rsm.s3.keys.LastModifiedReverseIndexKey;
import org.apache.kafka.rsm.s3.keys.LogFileKey;
import org.apache.kafka.rsm.s3.keys.MarkerKey;
import org.apache.kafka.rsm.s3.keys.OffsetIndexFileKey;
import org.apache.kafka.rsm.s3.keys.RemoteLogIndexFileKey;
import org.apache.kafka.rsm.s3.keys.TimeIndexFileKey;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import kafka.log.LogSegment;
import kafka.log.remote.RDI;
import kafka.log.remote.RemoteLogIndexEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopicPartitionCopying {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionCopying.class);

    private final String logPrefix;

    private final int leaderEpoch;

    private final TopicPartition topicPartition;
    private final LogSegment logSegment;

    private final String bucketName;
    private final TransferManager transferManager;

    private final long baseOffset;
    private final long lastOffset;
    private final List<RemoteLogIndexEntry> remoteLogIndexEntries;

    // TODO document locking / concurrency
    // TODO test locking / concurrency
    private final Object lock = new Object();
    private boolean cancelled = false;
    private volatile List<Upload> uploads = Collections.emptyList();

    private static final InputStream EMPTY_INPUT_STREAM = new InputStream() {
        @Override
        public int available() throws IOException {
            return 0;
        }

        @Override
        public int read() throws IOException {
            return -1;
        }
    };

    TopicPartitionCopying(TopicPartition topicPartition,
                          int leaderEpoch,
                          LogSegment logSegment,
                          String bucketName,
                          TransferManager transferManager,
                          int indexIntervalBytes) {
        this.logPrefix = "Topic-partition: " + topicPartition + ", leader epoch: " + leaderEpoch;

        this.leaderEpoch = leaderEpoch;
        this.topicPartition = topicPartition;
        this.logSegment = logSegment;
        this.bucketName = bucketName;
        this.transferManager = transferManager;

        baseOffset = logSegment.baseOffset();
        lastOffset = logSegment.readNextOffset() - 1;

        remoteLogIndexEntries = RemoteLogIndexer.index(
            logSegment.log().batches(),
            indexIntervalBytes,
            (firstBatch) ->
                RdiParsed.createRDI(
                    LogFileKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch),
                    firstBatch.position()
                )
        );
    }

    List<RemoteLogIndexEntry> copy() throws IOException {
        try {
            synchronized(lock) {
                if (cancelled) {
                    throwSegmentCopyingInterruptedException(null);
                }

                Upload lastModifiedReverseIndexFileUpload = uploadLastModifiedReverseIndexFile(logSegment);
                uploads = Collections.singletonList(lastModifiedReverseIndexFileUpload);
            }

            waitForAllUploads();

            synchronized(lock) {
                if (cancelled) {
                    throwSegmentCopyingInterruptedException(null);
                }

                Upload logFileUpload = uploadLogFile(logSegment);
                Upload offsetIndexFileUpload = uploadOffsetIndexLogFile(logSegment);
                Upload timeIndexFileUpload = uploadTimeIndexLogFile(logSegment);
                Upload remoteLogIndexUpload = uploadRemoteLogIndex(remoteLogIndexEntries);
                uploads = Arrays.asList(
                    logFileUpload, offsetIndexFileUpload, timeIndexFileUpload, remoteLogIndexUpload
                );
            }

            waitForAllUploads();

            synchronized(lock) {
                if (cancelled) {
                    throwSegmentCopyingInterruptedException(null);
                }
                Upload markerUpload = uploadMarker();
                uploads = Collections.singletonList(markerUpload);
            }

            waitForAllUploads();

            // TODO clean up in case of interruption

            return remoteLogIndexEntries;
        } catch (SdkClientException e) {
            throw new KafkaException("Error copying files for " + logSegment +
                " in " + topicPartition +
                " with leader epoch " + leaderEpoch, e);
        }
    }

    private Upload uploadLogFile(LogSegment logSegment) {
        final String key = LogFileKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading log file: {}", logPrefix, key);
        return uploadFile(key, logSegment.log().file());
    }

    private Upload uploadOffsetIndexLogFile(LogSegment logSegment) {
        final String key = OffsetIndexFileKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading offset index file: {}", logPrefix, key);
        return uploadFile(key, logSegment.offsetIndex().file());
    }

    private Upload uploadTimeIndexLogFile(LogSegment logSegment) {
        final String key = TimeIndexFileKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading time index file: {}", logPrefix, key);
        return uploadFile(key, logSegment.timeIndex().file());
    }

    private Upload uploadFile(String key, File file) {
        return transferManager.upload(bucketName, key, file);
    }

    private Upload uploadLastModifiedReverseIndexFile(LogSegment logSegment) {
        String key = LastModifiedReverseIndexKey.key(
            topicPartition, logSegment.lastModified(), baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading last modifier reverse index entry: {}", logPrefix, key);
        return uploadEmptyFile(key);
    }

    private Upload uploadRemoteLogIndex(List<RemoteLogIndexEntry> remoteLogIndexEntries) throws IOException {
        String key = RemoteLogIndexFileKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading remote index file: {}", logPrefix, key);
        int totalSize = 0;
        List<ByteBuffer> remoteLogIndexEntryBuffers = new ArrayList<>(remoteLogIndexEntries.size());
        for (RemoteLogIndexEntry remoteLogIndexEntry : remoteLogIndexEntries) {
            ByteBuffer buffer = remoteLogIndexEntry.asBuffer();
            remoteLogIndexEntryBuffers.add(buffer);
            totalSize += buffer.limit();
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(totalSize);
        try (InputStream inputStream = new GatheringByteBufferInputStream(remoteLogIndexEntryBuffers)) {
            return transferManager.upload(bucketName, key, inputStream, metadata);
        }
    }

    private Upload uploadMarker() {
        String key = MarkerKey.key(topicPartition, baseOffset, lastOffset, leaderEpoch);
        log.debug("[{}] Uploading marker: {}", logPrefix, key);
        return uploadEmptyFile(key);
    }

    private Upload uploadEmptyFile(String key) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        return transferManager.upload(bucketName, key, EMPTY_INPUT_STREAM, metadata);
    }

    private void waitForAllUploads() {
        try {
            for (Upload upload : uploads) {
                upload.waitForUploadResult();
            }
        } catch (InterruptedException | CancellationException e) {
            throwSegmentCopyingInterruptedException(e);
        }
    }

    void cancel() {
        synchronized (lock) {
            if (!cancelled) {
                log.debug("[{}] Cancelling uploads", logPrefix);
                cancelled = true;
                for (Upload upload : uploads) {
                    upload.abort();
                }
            } else {
                log.debug("[{}] Already cancelled", logPrefix);
            }
        }
    }

    private void throwSegmentCopyingInterruptedException(Throwable cause) {
        String message = "Copying of segment " + logSegment +
            " for topic-partition " + topicPartition +
            " in lead epoch " + leaderEpoch +
            " interrupted";
        KafkaException ex;
        if (cause != null) {
            ex = new KafkaException(message, cause);
        } else {
            ex = new KafkaException(message);
        }
        throw ex;
    }
}

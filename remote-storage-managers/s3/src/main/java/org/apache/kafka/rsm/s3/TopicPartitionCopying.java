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

import java.io.ByteArrayInputStream;
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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import kafka.log.Log;
import kafka.log.LogSegment;
import kafka.log.remote.RDI;
import kafka.log.remote.RemoteLogIndexEntry;

class TopicPartitionCopying {
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
                          LogSegment logSegment,
                          String bucketName,
                          TransferManager transferManager,
                          int indexIntervalBytes) {
        this.topicPartition = topicPartition;
        this.logSegment = logSegment;
        this.bucketName = bucketName;
        this.transferManager = transferManager;

        baseOffset = logSegment.baseOffset();
        lastOffset = logSegment.readNextOffset() - 1;

        remoteLogIndexEntries = RemoteLogIndexer.index(
            logSegment.log().batches(),
            indexIntervalBytes,
            (firstBatch) -> s3RDI(S3RemoteStorageManager.logFileKey(topicPartition, baseOffset, lastOffset), firstBatch.position())
        );
    }

    private RDI s3RDI(String s3Key, long position) {
        return new RDI((s3Key + S3RemoteStorageManager.RDI_POSITION_SEPARATOR + position).getBytes(StandardCharsets.UTF_8));
    }

    List<RemoteLogIndexEntry> copy() throws IOException {
        try {
            synchronized(lock) {
                if (cancelled) {
                    throwSegmentCopyingInterruptedException(null);
                }

                Upload logFileUpload = uploadFile(
                    S3RemoteStorageManager.logFileKey(topicPartition, baseOffset, lastOffset), logSegment.log().file());
                Upload offsetIndexFileUpload = uploadFile(
                    S3RemoteStorageManager.offsetIndexFileKey(topicPartition, baseOffset, lastOffset), logSegment.offsetIndex().file());
                Upload timeIndexFileUpload = uploadFile(
                    S3RemoteStorageManager.timestampIndexFileKey(topicPartition, baseOffset, lastOffset), logSegment.timeIndex().file());
                Upload largestTimestampReverseIndexFile = uploadLargestTimestampReverseIndexFile(logSegment);
                Upload remoteLogIndexUpload = uploadRemoteLogIndex(remoteLogIndexEntries);
                uploads = Arrays.asList(
                    logFileUpload, offsetIndexFileUpload, timeIndexFileUpload, largestTimestampReverseIndexFile, remoteLogIndexUpload
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
            throw new KafkaException("Error copying files for " + logSegment + " in " + topicPartition, e);
        }
    }

    private Upload uploadFile(String key, File file) {
        return transferManager.upload(bucketName, key, file);
    }

    private Upload uploadLargestTimestampReverseIndexFile(LogSegment logSegment) throws IOException {
        String key = S3RemoteStorageManager.largestTimestampReverseIndexFileKey(topicPartition, logSegment.largestTimestamp());
        String content = Log.filenamePrefixFromOffset(baseOffset) + "-" + Log.filenamePrefixFromOffset(lastOffset);
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
            return transferManager.upload(bucketName, key, inputStream, metadata);
        }
    }

    private Upload uploadRemoteLogIndex(List<RemoteLogIndexEntry> remoteLogIndexEntries) throws IOException {
        int totalSize = 0;
        List<ByteBuffer> remoteLogIndexEntryBuffers = new ArrayList<>(remoteLogIndexEntries.size());
        for (RemoteLogIndexEntry remoteLogIndexEntry : remoteLogIndexEntries) {
            ByteBuffer buffer = remoteLogIndexEntry.asBuffer();
            remoteLogIndexEntryBuffers.add(buffer);
            totalSize += buffer.limit();
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(totalSize);
        String key = S3RemoteStorageManager.remoteLogIndexFileKey(topicPartition, baseOffset, lastOffset);
        try (InputStream inputStream = new GatheringByteBufferInputStream(remoteLogIndexEntryBuffers)) {
            return transferManager.upload(bucketName, key, inputStream, metadata);
        }
    }

    private Upload uploadMarker() {
        String key = S3RemoteStorageManager.markerFileKey(topicPartition, baseOffset, lastOffset);
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
                cancelled = true;
                for (Upload upload : uploads) {
                    upload.abort();
                }
            }
        }
    }

    private void throwSegmentCopyingInterruptedException(Throwable cause) {
        String message = "Copying of segment " + logSegment + " for topic-partition " + topicPartition + " interrupted";
        KafkaException ex;
        if (cause != null) {
            ex = new KafkaException(message, cause);
        } else {
            ex = new KafkaException(message);
        }
        throw ex;
    }
}

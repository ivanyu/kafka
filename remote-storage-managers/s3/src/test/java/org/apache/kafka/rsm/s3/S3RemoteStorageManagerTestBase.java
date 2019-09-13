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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import kafka.log.Log;
import kafka.log.LogSegment;
import kafka.log.LogUtils;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.Before;

class S3RemoteStorageManagerTestBase {
    static final String TOPIC = "connect-log";
    static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    File logDir;

    @Before
    public void setUp() {
        logDir = TestUtils.tempDir();
    }

    @After
    public void tearDown() {
        try {
            Utils.delete(logDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Map<String, String> basicProps(String bucket) {
        Map<String, String> props = new HashMap<>();
        props.put(S3RemoteStorageManagerConfig.S3_BUCKET_NAME_CONFIG, bucket);
        return props;
    }

    LogSegment createLogSegment(long offset) {
        return LogUtils.createSegment(offset, logDir, 4096, Time.SYSTEM);
    }

    void appendRecordBatch(LogSegment segment, long offset, int recordSize, int numRecords) {
        SimpleRecord[] records = new SimpleRecord[numRecords];
        long timestamp = 0;
        for (int i = 0; i < numRecords; i++) {
            timestamp = (offset + i) * 1000;
            records[i] = new SimpleRecord(timestamp, new byte[recordSize]);
            records[i].value().putLong(0, offset + i);
            records[i].value().rewind();
        }
        long lastOffset = offset + numRecords - 1;
        MemoryRecords memoryRecords = MemoryRecords.withRecords(
            RecordBatch.CURRENT_MAGIC_VALUE, offset, CompressionType.NONE, TimestampType.CREATE_TIME, records);
        segment.append(lastOffset, timestamp, offset, memoryRecords);
    }

    void appendControlBatch(LogSegment segment, long offset) {
        EndTransactionMarker endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0);
        MemoryRecords records = MemoryRecords.withEndTransactionMarker(offset, RecordBatch.NO_TIMESTAMP, 0, 0L,
            (short) 0, endTxnMarker);
        segment.append(records.batchIterator().peek().lastOffset(), RecordBatch.NO_TIMESTAMP, -1L, records);
    }

    String lastModifiedReverseIndexS3Key(TopicPartition topicPartition,
                                         long lastModifiedMs, long baseOffset, long lastOffset, int leaderEpoch) {
        return topicPartition + "/last-modified-reverse-index/" +
            Log.filenamePrefixFromOffset(lastModifiedMs) + "-" +
            Log.filenamePrefixFromOffset(baseOffset) + "-" +
            Log.filenamePrefixFromOffset(lastOffset) +
            "-le" + leaderEpoch;
    }

    String s3Key(TopicPartition topicPartition, String dir, long baseOffset, long lastOffset, int leaderEpoch) {
        return topicPartition + "/" + dir + "/" +
            Log.filenamePrefixFromOffset(baseOffset) + "-" +
            Log.filenamePrefixFromOffset(lastOffset) +
            "-le" + leaderEpoch;
    }
}

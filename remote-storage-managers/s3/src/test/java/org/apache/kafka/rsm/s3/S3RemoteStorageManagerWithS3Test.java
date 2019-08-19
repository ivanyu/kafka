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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import kafka.log.Log;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class S3RemoteStorageManagerWithS3Test extends S3RemoteStorageManagerTestBase {

    private int NORMAL_RECORD_SIZE = 20;
    private int NORMAL_BATCH_RECORD_COUNT = 10;
    private int CONTROL_BATCH_RECORD_COUNT = 1;

    @ClassRule
    public static LocalStackContainer localstack = new LocalStackContainer().withServices(S3);
    private static AmazonS3 s3Client;

    private String bucket;
    private S3RemoteStorageManager remoteStorageManager;

    @BeforeClass
    public static void setUpClass() {
        s3Client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localstack.getEndpointConfiguration(S3))
            .build();
    }

    @Before
    public void setUp() {
        super.setUp();
        bucket = TestUtils.randomString(10).toLowerCase();
        s3Client.createBucket(bucket);
        remoteStorageManager = new S3RemoteStorageManager(localstack.getEndpointConfiguration(S3), 2);
    }

    @After
    public void tearDown() {
        super.tearDown();
        remoteStorageManager.close();
    }

    @AfterClass
    public static void tearDownClass() {
        s3Client.shutdown();
    }

    @Test
    public void testEarliestLogOffsetWhenEmpty() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));
        assertEquals(-1L, remoteStorageManager.earliestLogOffset(TP0));
    }

    @Test
    public void testEarliestLogOffset() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();
        assertEquals(segmentsOnS3Setup.baseOffset,
            remoteStorageManager.earliestLogOffset(segmentsOnS3Setup.topicPartition));
    }

    @Test
    public void testCopy() throws IOException {
        LogSegment segment1 = createLogSegment(0);
        appendRecordBatch(segment1, 0, 10, 10);
        appendRecordBatch(segment1, 10, 10, 10);
        appendRecordBatch(segment1, 20, 10, 10);
        segment1.onBecomeInactiveSegment();

        LogSegment segment2 = createLogSegment(5);
        appendRecordBatch(segment2, 5, 10, 10);
        appendRecordBatch(segment2, 15, 10, 10);
        appendRecordBatch(segment2, 25, 10, 10);
        segment1.onBecomeInactiveSegment();

        Map<String, String> props = basicProps(bucket);
        props.put(S3RemoteStorageManagerConfig.INDEX_INTERVAL_BYTES_CONFIG, "1");
        remoteStorageManager.configure(props);

        List<RemoteLogIndexEntry> remoteLogIndexEntries1 = remoteStorageManager.copyLogSegment(TP0, segment1);
        assertEquals(3, remoteLogIndexEntries1.size());

        assertEquals(0, remoteLogIndexEntries1.get(0).firstOffset());
        assertEquals(9, remoteLogIndexEntries1.get(0).lastOffset());
        assertEquals(0, remoteLogIndexEntries1.get(0).firstTimeStamp());
        assertEquals(9000, remoteLogIndexEntries1.get(0).lastTimeStamp());

        assertEquals(10, remoteLogIndexEntries1.get(1).firstOffset());
        assertEquals(19, remoteLogIndexEntries1.get(1).lastOffset());
        assertEquals(10000, remoteLogIndexEntries1.get(1).firstTimeStamp());
        assertEquals(19000, remoteLogIndexEntries1.get(1).lastTimeStamp());

        assertEquals(20, remoteLogIndexEntries1.get(2).firstOffset());
        assertEquals(29, remoteLogIndexEntries1.get(2).lastOffset());
        assertEquals(20000, remoteLogIndexEntries1.get(2).firstTimeStamp());
        assertEquals(29000, remoteLogIndexEntries1.get(2).lastTimeStamp());

        List<RemoteLogIndexEntry> remoteLogIndexEntries2 = remoteStorageManager.copyLogSegment(TP1, segment2);
        assertEquals(3, remoteLogIndexEntries2.size());
        assertEquals(5, remoteLogIndexEntries2.get(0).firstOffset());
        assertEquals(14, remoteLogIndexEntries2.get(0).lastOffset());
        assertEquals(5000, remoteLogIndexEntries2.get(0).firstTimeStamp());
        assertEquals(14000, remoteLogIndexEntries2.get(0).lastTimeStamp());

        assertEquals(15, remoteLogIndexEntries2.get(1).firstOffset());
        assertEquals(24, remoteLogIndexEntries2.get(1).lastOffset());
        assertEquals(15000, remoteLogIndexEntries2.get(1).firstTimeStamp());
        assertEquals(24000, remoteLogIndexEntries2.get(1).lastTimeStamp());

        assertEquals(25, remoteLogIndexEntries2.get(2).firstOffset());
        assertEquals(34, remoteLogIndexEntries2.get(2).lastOffset());
        assertEquals(25000, remoteLogIndexEntries2.get(2).firstTimeStamp());
        assertEquals(34000, remoteLogIndexEntries2.get(2).lastTimeStamp());

        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", 0, 29),
            s3Key(TP0, "index", 0, 29),
            s3Key(TP0, "time-index", 0, 29),
            s3Key(TP0, "remote-log-index", 0, 29),
            TP0 + "/largest-timestamp-reverse-index/" + Log.filenamePrefixFromOffset(segment1.largestTimestamp()),
            s3Key(TP0, "marker", 0, 29),
            s3Key(TP1, "log", 5, 34),
            s3Key(TP1, "index", 5, 34),
            s3Key(TP1, "time-index", 5, 34),
            s3Key(TP1, "remote-log-index", 5, 34),
            TP1 + "/largest-timestamp-reverse-index/" + Log.filenamePrefixFromOffset(segment2.largestTimestamp()),
            s3Key(TP1, "marker", 5, 34)
        ));
    }

    // TODO think out the overwriting logic and change the test
    @Test
    public void testOverwrite() throws IOException {
        LogSegment segment1 = createLogSegment(0);
        appendRecordBatch(segment1, 0, 10, 100);
        segment1.onBecomeInactiveSegment();

        Map<String, String> props = basicProps(bucket);
        props.put(S3RemoteStorageManagerConfig.INDEX_INTERVAL_BYTES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        remoteStorageManager.configure(props);

        remoteStorageManager.copyLogSegment(TP0, segment1);
        remoteStorageManager.copyLogSegment(TP0, segment1);

        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", 0, 99),
            s3Key(TP0, "index", 0, 99),
            s3Key(TP0, "time-index", 0, 99),
            s3Key(TP0, "remote-log-index", 0, 99),
            TP0 + "/largest-timestamp-reverse-index/" + Log.filenamePrefixFromOffset(segment1.largestTimestamp()),
            s3Key(TP0, "marker", 0, 99)
        ));
    }

    @Test
    public void testReadFromFirstOffset() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;

        int maxBytes = Integer.MAX_VALUE;
        Records readRecords = remoteStorageManager.read(
            remoteLogIndexEntries.get(0), maxBytes, 0, true);

        // Offset 0 goes to the 1st index entry, 1st batch.
        // All the batches associated with this entry must be returned.
        // 4 first batches go into a single log entry.
        // 3 normal batches of 10 records + 1 control batch.
        assertEquals(NORMAL_BATCH_RECORD_COUNT * 3 + CONTROL_BATCH_RECORD_COUNT, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 0);
        checkNormalBatch(readRecords, 1, 10);
        checkNormalBatch(readRecords, 2, 20);
        checkControlBatch(readRecords, 3, 21);
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    @Test
    public void testReadFromNotFirstOffsetInFirstBatch() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;

        int maxBytes = Integer.MAX_VALUE;
        Records readRecords = remoteStorageManager.read(
            remoteLogIndexEntries.get(0), maxBytes, 5, true);

        // Offset 5 goes to the 1st index entry, 1st batch.
        // All 4 batches associated with this entry must be returned.
        // 3 normal batches of 10 records + 1 control batch.
        assertEquals(NORMAL_BATCH_RECORD_COUNT * 3 + CONTROL_BATCH_RECORD_COUNT, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 0);
        checkNormalBatch(readRecords, 1, 10);
        checkNormalBatch(readRecords, 2, 20);
        checkControlBatch(readRecords, 3, 21);
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    @Test
    public void testReadFromSecondBatch() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;

        int maxBytes = Integer.MAX_VALUE;
        Records readRecords = remoteStorageManager.read(
            remoteLogIndexEntries.get(0), maxBytes, 10, true);

        // Offset 10 goes to the 1st index entry, 2nd batch.
        // Batches 2-4 associated with this entry must be returned.
        // 2 normal batches of 10 records + 1 control batch.
        assertEquals(NORMAL_BATCH_RECORD_COUNT * 2 + CONTROL_BATCH_RECORD_COUNT, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 10);
        checkNormalBatch(readRecords, 1, 20);
        checkControlBatch(readRecords, 2, 21);
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    @Test
    public void testReadBeyondLastOffset() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;
        Throwable e = assertThrows(IllegalArgumentException.class,
            () -> remoteStorageManager.read(remoteLogIndexEntries.get(0), Integer.MAX_VALUE, 100, true));
        assertEquals("startOffset > remoteLogIndexEntry.lastOffset(): 100 > 21", e.getMessage());
    }

    @Test
    public void testReadMaxBytesLimitNoCompleteBatchMinOneRecordFalse() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;

        int maxBytes = 1;
        Records readRecords = remoteStorageManager.read(
            remoteLogIndexEntries.get(0), maxBytes, 0, false);

        assertEquals(0, countRecords(readRecords));
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    @Test
    public void testReadMaxBytesLimitNoCompleteBatchMinOneRecordTrue() throws IOException {
        List<RemoteLogIndexEntry> remoteLogIndexEntries = uploadSegments().remoteLogIndexEntries;

        int maxBytes = 1;
        Records readRecords = remoteStorageManager.read(
            remoteLogIndexEntries.get(0), maxBytes, 0, true);

        assertEquals(1, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 0, 1);
        // minOneRecord == true, we don't check the returned size
    }

    @Test
    public void testReadMaxBytesLimitOneCompleteBatchMinOneRecordFalse() throws IOException {
        testMaxBytesLimitOneCompleteBatch(false);
    }

    @Test
    public void testReadMaxBytesLimitOneCompleteBatchMinOneRecordTrue() throws IOException {
        testMaxBytesLimitOneCompleteBatch(true);
    }

    @Test
    public void testReadWithoutMarker() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();

        deleteMarker(segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset);

        Records readRecords = remoteStorageManager.read(
            segmentsOnS3Setup.remoteLogIndexEntries.get(0), Integer.MAX_VALUE, 0, true);
        assertEquals(NORMAL_BATCH_RECORD_COUNT * 3 + CONTROL_BATCH_RECORD_COUNT, countRecords(readRecords));
    }

    @Test
    public void testReadWithoutLogFile() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();

        deleteLogFile(segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset);

        Throwable e = assertThrows(KafkaException.class,
            () -> remoteStorageManager.read(segmentsOnS3Setup.remoteLogIndexEntries.get(0), Integer.MAX_VALUE, 0, true));
        assertEquals(
            "Error reading log file " +
                s3Key(segmentsOnS3Setup.topicPartition, "log", segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset),
            e.getMessage());
    }

    @Test
    public void testListRemoteSegments() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        int recordsInSegment = 20;
        int numSegments = 10;
        for (int i = 0; i < numSegments; i++) {
            int offset = recordsInSegment * i;
            LogSegment segment = createLogSegment(offset);
            appendRecordBatch(segment, offset, 100, recordsInSegment);
            segment.onBecomeInactiveSegment();
            remoteStorageManager.copyLogSegment(TP0, segment);
        }

        List<RemoteLogSegmentInfo> allRemoteSegments = remoteStorageManager.listRemoteSegments(TP0);
        assertEquals(numSegments, allRemoteSegments.size());
        for (int i = 0; i < numSegments; i++) {
            RemoteLogSegmentInfo segment = allRemoteSegments.get(i);
            assertEquals(recordsInSegment * i, segment.baseOffset());
            assertEquals(recordsInSegment * (i + 1) - 1, segment.endOffset());
        }

        int numSkippedSegments = 5;
        List<RemoteLogSegmentInfo> partialRemoteSegments = remoteStorageManager.listRemoteSegments(
            TP0, numSkippedSegments * recordsInSegment);
        assertEquals(numSkippedSegments, partialRemoteSegments.size());
        for (int i = numSkippedSegments; i < numSegments; i++) {
            RemoteLogSegmentInfo segment = partialRemoteSegments.get(i - numSkippedSegments);
            assertEquals(recordsInSegment * i, segment.baseOffset());
            assertEquals(recordsInSegment * (i + 1) - 1, segment.endOffset());
        }
    }

    @Test
    public void testListRemoteSegmentsDoesNotListSegmentsWithoutMarker() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        int recordsInSegment = 10;
        int numSegments = 2;
        for (int i = 0; i < numSegments; i++) {
            int offset = recordsInSegment * i;
            LogSegment segment = createLogSegment(offset);
            appendRecordBatch(segment, offset, 100, recordsInSegment);
            segment.onBecomeInactiveSegment();
            remoteStorageManager.copyLogSegment(TP0, segment);
        }

        deleteMarker(0, 9);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            1 * recordsInSegment, recordsInSegment * 2 - 1, TP0, Collections.emptyMap());
        assertThat(
            remoteStorageManager.listRemoteSegments(TP0),
            containsInAnyOrder(segmentInfo)
        );
    }

    @Test
    public void testDeleteTopicPartition() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();
        remoteStorageManager.deleteTopicPartition(segmentsOnS3Setup.topicPartition);
        assertThat(listS3Keys(), empty());
    }

    @Test
    public void testGetRemoteLogIndexEntries() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();
        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset, segmentsOnS3Setup.topicPartition,
            Collections.emptyMap());
        List<RemoteLogIndexEntry> result = remoteStorageManager.getRemoteLogIndexEntries(segmentInfo);
        assertEquals(segmentsOnS3Setup.remoteLogIndexEntries, result);
    }

    @Test
    public void testGetRemoteLogIndexEntriesWithoutMarker() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();

        deleteMarker(segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset, segmentsOnS3Setup.topicPartition,
            Collections.emptyMap());
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.getRemoteLogIndexEntries(segmentInfo));
        assertEquals("Marker for " + segmentInfo + " doesn't exist", e.getMessage());
    }

    @Test
    public void testGetRemoteLogIndexEntriesNoIndexFile() throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();

        deleteRemoteLogIndexFile(segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset, segmentsOnS3Setup.topicPartition,
            Collections.emptyMap());
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.getRemoteLogIndexEntries(segmentInfo));
        assertEquals(
            "Error reading remote log index file " +
                s3Key(segmentsOnS3Setup.topicPartition, "remote-log-index", segmentsOnS3Setup.baseOffset, segmentsOnS3Setup.lastOffset),
            e.getMessage());
    }

    @Test
    public void testGetRemoteLogIndexEntriesCorruptedIndexFile() {
        // TODO implement
        throw new RuntimeException("not implemented");
    }

    private void testMaxBytesLimitOneCompleteBatch(boolean minOneRecord) throws IOException {
        SegmentsOnS3Setup segmentsOnS3Setup = uploadSegments();

        int maxBytes = segmentsOnS3Setup.batchSizesInBytes.get(0) + 1;
        Records readRecords = remoteStorageManager.read(
            segmentsOnS3Setup.remoteLogIndexEntries.get(0),
            maxBytes, 0, minOneRecord);

        assertEquals(NORMAL_BATCH_RECORD_COUNT, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 0);
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    private SegmentsOnS3Setup uploadSegments() throws IOException {
        LogSegment segment = createLogSegment(0);
        appendRecordBatch(segment, 0, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 10, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 20, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, 21);
        appendRecordBatch(segment, 31, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 41, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 51, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, 52);
        appendRecordBatch(segment, 62, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 72, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, 82, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, 83);
        segment.onBecomeInactiveSegment();

        Map<String, String> props = basicProps(bucket);
        List<Integer> batchSizesInByte = StreamSupport.stream(segment.log().batches().spliterator(), false)
            .map(FileLogInputStream.FileChannelRecordBatch::sizeInBytes)
            .collect(Collectors.toList());
        props.put(S3RemoteStorageManagerConfig.INDEX_INTERVAL_BYTES_CONFIG,
            Integer.toString(batchSizesInByte.get(0) + batchSizesInByte.get(1) + batchSizesInByte.get(2) + batchSizesInByte.get(3)));
        remoteStorageManager.configure(props);

        List<RemoteLogIndexEntry> logIndexEntries = remoteStorageManager.copyLogSegment(TP0, segment);
        return new SegmentsOnS3Setup(TP0, logIndexEntries, batchSizesInByte, 0, 83);
    }

    private int countRecords(Records records) {
        int result = 0;
        for (Record record : records.records()) {
            result += 1;
        }
        return result;
    }

    private void checkNormalBatch(Records records, long batchIdx, long baseOffset) {
        checkNormalBatch(records, batchIdx, baseOffset, NORMAL_BATCH_RECORD_COUNT);
    }

    private void checkNormalBatch(Records records, long batchIdx, long baseOffset, int expectedRecordCount) {
        RecordBatch recordBatch = StreamSupport.stream(records.batches().spliterator(), false)
            .skip(batchIdx)
            .findFirst()
            .get();
        recordBatch.ensureValid();
        assertEquals(baseOffset, recordBatch.baseOffset());

        int recordCount = 0;
        int i = 0;
        for (Record record : recordBatch) {
            record.ensureValid();
            assertEquals(NORMAL_RECORD_SIZE, record.valueSize());
            assertEquals(baseOffset + i, record.offset());
            assertEquals((baseOffset + i) * 1000, record.timestamp());
            assertEquals(baseOffset + i, record.value().getLong(0));

            recordCount += 1;
            i += 1;
        }
        assertEquals(expectedRecordCount, recordCount);
    }

    private void checkControlBatch(Records records, long batchIdx, long baseOffset) {
        RecordBatch recordBatch = StreamSupport.stream(records.batches().spliterator(), false)
            .skip(batchIdx)
            .findFirst()
            .get();
        recordBatch.ensureValid();
        assertEquals(baseOffset, recordBatch.baseOffset());
        assertTrue(recordBatch.isControlBatch());

        int recordCount = 0;
        for (Record record : recordBatch) {
            record.ensureValid();
            recordCount += 1;
        }
        assertEquals(CONTROL_BATCH_RECORD_COUNT, recordCount);
    }

    private void deleteMarker(long baseOffset, long lastOffset) {
        assert baseOffset <= lastOffset;
        s3Client.deleteObject(bucket, s3Key(TP0, "marker", baseOffset, lastOffset));
    }

    private void deleteLogFile(long baseOffset, long lastOffset) {
        assert baseOffset <= lastOffset;
        s3Client.deleteObject(bucket, s3Key(TP0, "log", baseOffset, lastOffset));
    }

    private void deleteRemoteLogIndexFile(long baseOffset, long lastOffset) {
        assert baseOffset <= lastOffset;
        s3Client.deleteObject(bucket, s3Key(TP0, "remote-log-index", baseOffset, lastOffset));
    }

    private List<String> listS3Keys() {
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket).getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private static class SegmentsOnS3Setup {
        final TopicPartition topicPartition;
        final List<RemoteLogIndexEntry> remoteLogIndexEntries;
        final List<Integer> batchSizesInBytes;
        final long baseOffset;
        final long lastOffset;

        SegmentsOnS3Setup(TopicPartition topicPartition,
                          List<RemoteLogIndexEntry> remoteLogIndexEntries,
                          List<Integer> batchSizesInBytes,
                          long baseOffset, long lastOffset) {
            this.topicPartition = topicPartition;
            this.remoteLogIndexEntries = new ArrayList<>(remoteLogIndexEntries);
            this.batchSizesInBytes = new ArrayList<>(batchSizesInBytes);
            this.baseOffset = baseOffset;
            this.lastOffset = lastOffset;
        }
    }
}

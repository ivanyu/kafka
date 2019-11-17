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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class S3RemoteStorageManagerWithS3Test extends S3RemoteStorageManagerTestBase {

    private static final int NORMAL_RECORD_SIZE = 20;
    private static final int NORMAL_BATCH_RECORD_COUNT = 10;
    private static final int CONTROL_BATCH_RECORD_COUNT = 1;

    private Long segmentFileLastModified = null;

    @ClassRule
    public static LocalStackContainer localstack = new LocalStackContainer().withServices(S3);
    private static AmazonS3 s3Client;

    private String bucket;
    private S3RemoteStorageManager remoteStorageManager;

    @BeforeClass
    public static void setUpClass() {
        s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AnonymousCredentialsProvider())
            .withEndpointConfiguration(localstack.getEndpointConfiguration(S3))
            .build();
    }

    @Before
    public void setUp() {
        super.setUp();
        bucket = TestUtils.randomString(10).toLowerCase(Locale.ROOT);
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
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        assertEquals(segmentOnS3Setup.baseOffset,
            remoteStorageManager.earliestLogOffset(segmentOnS3Setup.topicPartition));
    }

    @Test
    public void testEarliestLogOffsetMultipleLeaderEpochs() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup1 = uploadSegment(0, 1000, true);
        SegmentOnS3Setup segmentOnS3Setup2 = uploadSegment(1, 0, true);
        assertEquals(segmentOnS3Setup2.baseOffset,
            remoteStorageManager.earliestLogOffset(segmentOnS3Setup2.topicPartition));
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

        List<RemoteLogIndexEntry> remoteLogIndexEntries1 = remoteStorageManager.copyLogSegment(TP0, segment1, 0);
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

        List<RemoteLogIndexEntry> remoteLogIndexEntries2 = remoteStorageManager.copyLogSegment(TP1, segment2, 1);
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
            s3Key(TP0, "log", 0, 29, 0),
            s3Key(TP0, "index", 0, 29, 0),
            s3Key(TP0, "time-index", 0, 29, 0),
            s3Key(TP0, "remote-log-index", 0, 29, 0),
            lastModifiedReverseIndexS3Key(TP0, segment1.lastModified(), 0, 29, 0),
            s3Key(TP1, "log", 5, 34, 1),
            s3Key(TP1, "index", 5, 34, 1),
            s3Key(TP1, "time-index", 5, 34, 1),
            s3Key(TP1, "remote-log-index", 5, 34, 1),
            lastModifiedReverseIndexS3Key(TP1, segment2.lastModified(), 5, 34, 1)
        ));
    }

    @Test
    public void testCopyEmptySegment() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        LogSegment segment = createLogSegment(0);
        segment.onBecomeInactiveSegment();

        Throwable e = assertThrows(AssertionError.class, () -> remoteStorageManager.copyLogSegment(TP0, segment, 0));
        assertEquals("Log segment size must be > 0", e.getMessage());
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

        remoteStorageManager.copyLogSegment(TP0, segment1, 0);
        remoteStorageManager.copyLogSegment(TP0, segment1, 0);

        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", 0, 99, 0),
            s3Key(TP0, "index", 0, 99, 0),
            s3Key(TP0, "time-index", 0, 99, 0),
            s3Key(TP0, "remote-log-index", 0, 99, 0),
            lastModifiedReverseIndexS3Key(TP0, segment1.lastModified(), 0, 99, 0)
        ));
    }

    @Test
    public void testCleanupLogWhenEmpty() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));
        assertEquals(-1L, remoteStorageManager.cleanupLogUntil(TP0, Long.MAX_VALUE));
    }

    @Test
    public void testCleanupLogBeforeTheEarliest() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        assertEquals(segmentOnS3Setup.baseOffset, remoteStorageManager.cleanupLogUntil(TP0, 0));

        // All files must remain in place.
        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            s3Key(TP0, "index", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            s3Key(TP0, "time-index", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            s3Key(TP0, "remote-log-index", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            lastModifiedReverseIndexS3Key(TP0, segmentOnS3Setup.segment.lastModified(), segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch)
        ));
    }

    @Test
    public void testCleanupLogExactlyMatchTimestamp() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        assertEquals(-1L, remoteStorageManager.cleanupLogUntil(TP0, segmentOnS3Setup.segment.lastModified()));
        assertThat(listS3Keys(), empty());
    }

    @Test
    public void testCleanupLogBetweenSegments() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup1 = uploadSegment(0, 0, true);
        SegmentOnS3Setup segmentOnS3Setup2 = uploadSegment(0, 1000, false);
        assertEquals(1000L, remoteStorageManager.cleanupLogUntil(TP0, segmentOnS3Setup1.segment.lastModified() + 1));

        // All files of the second segment must remain in place.
        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "time-index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "remote-log-index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            lastModifiedReverseIndexS3Key(TP0, segmentOnS3Setup2.segment.lastModified(), segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch)
        ));
    }

    @Test
    public void testCleanupLogAfterAllSegments() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup1 = uploadSegment(0, 0, true);
        SegmentOnS3Setup segmentOnS3Setup2 = uploadSegment(0, 1000, false);
        SegmentOnS3Setup segmentOnS3Setup3 = uploadSegment(0, 2000, false);
        SegmentOnS3Setup segmentOnS3Setup4 = uploadSegment(0, 4000, false);
        assertEquals(-1L, remoteStorageManager.cleanupLogUntil(TP0, segmentOnS3Setup4.segment.lastModified() + 1));
        assertThat(listS3Keys(), empty());
    }

    @Test
    public void testCleanupLogPartiallyDeletedBefore() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup1 = uploadSegment(0, 0, true);
        SegmentOnS3Setup segmentOnS3Setup2 = uploadSegment(0, 1000, false);

        deleteLogFile(segmentOnS3Setup1.baseOffset, segmentOnS3Setup1.lastOffset, segmentOnS3Setup1.leaderEpoch);

        assertEquals(1000L, remoteStorageManager.cleanupLogUntil(TP0, segmentOnS3Setup1.segment.lastModified() + 1));

        // All files of the second segment must remain in place.
        List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, "log", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "time-index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            s3Key(TP0, "remote-log-index", segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch),
            lastModifiedReverseIndexS3Key(TP0, segmentOnS3Setup2.segment.lastModified(), segmentOnS3Setup2.baseOffset, segmentOnS3Setup2.lastOffset, segmentOnS3Setup2.leaderEpoch)
        ));
    }

    @Test
    public void testReadFromFirstOffset() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = Integer.MAX_VALUE;

        Iterator<FileLogInputStream.FileChannelRecordBatch> batchIterator = segmentOnS3Setup.segment.log().batchIterator();
        for (RemoteLogIndexEntry remoteLogIndexEntry : segmentOnS3Setup.remoteLogIndexEntries) {
            Records readRecords = remoteStorageManager.read(
                remoteLogIndexEntry,
                maxBytes,
                remoteLogIndexEntry.firstOffset(), true);

            // All the batches associated with this entry must be returned.
            // 4 first batches go into a single log entry.
            // 3 normal batches of 10 records + 1 control batch.
            assertEquals(NORMAL_BATCH_RECORD_COUNT * 3 + CONTROL_BATCH_RECORD_COUNT, countRecords(readRecords));
            checkNormalBatch(readRecords, 0, batchIterator.next().baseOffset());
            checkNormalBatch(readRecords, 1, batchIterator.next().baseOffset());
            checkNormalBatch(readRecords, 2, batchIterator.next().baseOffset());
            checkControlBatch(readRecords, 3, batchIterator.next().baseOffset());
            assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
        }
    }

    @Test
    public void testReadFromNotFirstOffsetInFirstBatch() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = Integer.MAX_VALUE;
        Records readRecords = remoteStorageManager.read(
            segmentOnS3Setup.remoteLogIndexEntries.get(0), maxBytes, 5, true);

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
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = Integer.MAX_VALUE;
        Records readRecords = remoteStorageManager.read(
            segmentOnS3Setup.remoteLogIndexEntries.get(0), maxBytes, 10, true);

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
        final SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        Throwable e = assertThrows(IllegalArgumentException.class,
            () -> remoteStorageManager.read(
                segmentOnS3Setup.remoteLogIndexEntries.get(0), Integer.MAX_VALUE, 100, true));
        assertEquals("startOffset > remoteLogIndexEntry.lastOffset(): 100 > 21", e.getMessage());
    }

    @Test
    public void testReadMaxBytesLimitNoCompleteBatchMinOneRecordFalse() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = 1;
        Records readRecords = remoteStorageManager.read(
            segmentOnS3Setup.remoteLogIndexEntries.get(0), maxBytes, 0, false);

        assertEquals(0, countRecords(readRecords));
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    @Test
    public void testReadMaxBytesLimitNoCompleteBatchMinOneRecordTrue() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = 1;
        Records readRecords = remoteStorageManager.read(
            segmentOnS3Setup.remoteLogIndexEntries.get(0), maxBytes, 0, true);

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
    public void testReadWithoutLogFile() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        deleteLogFile(segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch);

        Throwable e = assertThrows(KafkaException.class,
            () -> remoteStorageManager.read(
                segmentOnS3Setup.remoteLogIndexEntries.get(0), Integer.MAX_VALUE, 0, true));
        assertEquals(
            "Error reading log file " +
                s3Key(segmentOnS3Setup.topicPartition, "log", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            e.getMessage());
    }

    @Test
    public void testBucketMigration() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        String bucket2 = bucket + "-new";
        s3Client.createBucket(bucket2);

        ObjectListing objectListing = s3Client.listObjects(bucket);
        do {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                s3Client.copyObject(bucket, objectSummary.getKey(), bucket2, objectSummary.getKey());
            }
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        } while (true);

        try (S3RemoteStorageManager remoteStorageManager2 =
                 new S3RemoteStorageManager(localstack.getEndpointConfiguration(S3), null)) {
            remoteStorageManager2.configure(basicProps(bucket2));

            assertEquals(segmentOnS3Setup.baseOffset, remoteStorageManager2.earliestLogOffset(segmentOnS3Setup.topicPartition));

            List<RemoteLogSegmentInfo> remoteLogSegmentInfos = remoteStorageManager2.listRemoteSegments(segmentOnS3Setup.topicPartition);
            assertEquals(1, remoteLogSegmentInfos.size());
            RemoteLogSegmentInfo expectedLogSegmentInfo = new RemoteLogSegmentInfo(
                segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset,
                segmentOnS3Setup.topicPartition,
                segmentOnS3Setup.leaderEpoch, Collections.emptyMap());
            assertEquals(expectedLogSegmentInfo, remoteLogSegmentInfos.get(0));

            int maxBytes = Integer.MAX_VALUE;
            List<Record> readRecords = new ArrayList<>();
            for (RemoteLogIndexEntry remoteLogIndexEntry : segmentOnS3Setup.remoteLogIndexEntries) {
                Records records = remoteStorageManager2.read(
                    remoteLogIndexEntry, maxBytes, remoteLogIndexEntry.firstOffset(), true);
                records.records().forEach(readRecords::add);
            }
            Iterator<Record> readRecordsIter = readRecords.iterator();
            Iterator<Record> expectedRecordsIter = segmentOnS3Setup.segment.log().records().iterator();
            while (expectedRecordsIter.hasNext()) {
                assert readRecordsIter.hasNext();
                assertEquals(expectedRecordsIter.next(), readRecordsIter.next());
            }
            assert !readRecordsIter.hasNext();
        }
    }

    @Test
    public void testListRemoteSegments() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        final int numRecords = 20;

        // baseOffset=0, lastOffset=19, leaderEpoch=0
        int offset = 0;
        LogSegment segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 0);

        // baseOffset=5, lastOffset=24, leaderEpoch=1
        offset = 5;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 1);

        // baseOffset=20, lastOffset=39, leaderEpoch=0
        offset = 20;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 0);

        // baseOffset=25, lastOffset=44, leaderEpoch=1
        offset = 25;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 1);

        // baseOffset=40, lastOffset=59, leaderEpoch=0
        offset = 40;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 0);

        // baseOffset=45, lastOffset=64, leaderEpoch=1
        offset = 45;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, numRecords);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 1);

        List<RemoteLogSegmentInfo> allRemoteSegments = remoteStorageManager.listRemoteSegments(TP0);
        assertEquals(6, allRemoteSegments.size());

        RemoteLogSegmentInfo segmentInfo = allRemoteSegments.get(0);
        assertEquals(0, segmentInfo.baseOffset());
        assertEquals(19, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(1);
        assertEquals(5, segmentInfo.baseOffset());
        assertEquals(24, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(2);
        assertEquals(numRecords, segmentInfo.baseOffset());
        assertEquals(39, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(3);
        assertEquals(25, segmentInfo.baseOffset());
        assertEquals(44, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(4);
        assertEquals(40, segmentInfo.baseOffset());
        assertEquals(59, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(5);
        assertEquals(45, segmentInfo.baseOffset());
        assertEquals(64, segmentInfo.lastOffset());

        List<RemoteLogSegmentInfo> partialRemoteSegments = remoteStorageManager.listRemoteSegments(
            TP0, 41);
        assertEquals(3, partialRemoteSegments.size());
        segmentInfo = partialRemoteSegments.get(0);
        assertEquals(25, segmentInfo.baseOffset());
        assertEquals(44, segmentInfo.lastOffset());

        segmentInfo = partialRemoteSegments.get(1);
        assertEquals(40, segmentInfo.baseOffset());
        assertEquals(59, segmentInfo.lastOffset());

        segmentInfo = partialRemoteSegments.get(2);
        assertEquals(45, segmentInfo.baseOffset());
        assertEquals(64, segmentInfo.lastOffset());
    }

    @Test
    public void testListRemoteSegmentsOrder() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        // baseOffset=1, lastOffset=18, leaderEpoch=0
        int offset = 1;
        LogSegment segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, 18);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 0);

        // baseOffset=0, lastOffset=20, leaderEpoch=1
        offset = 0;
        segment = createLogSegment(offset);
        appendRecordBatch(segment, offset, 100, 21);
        segment.onBecomeInactiveSegment();
        remoteStorageManager.copyLogSegment(TP0, segment, 1);

        List<RemoteLogSegmentInfo> allRemoteSegments = remoteStorageManager.listRemoteSegments(TP0);
        assertEquals(2, allRemoteSegments.size());

        // Should be sorted by base offset.
        RemoteLogSegmentInfo segmentInfo = allRemoteSegments.get(0);
        assertEquals(0, segmentInfo.baseOffset());
        assertEquals(20, segmentInfo.lastOffset());

        segmentInfo = allRemoteSegments.get(1);
        assertEquals(1, segmentInfo.baseOffset());
        assertEquals(18, segmentInfo.lastOffset());
    }

    @Test
    public void testListRemoteSegmentsWithMultipleLeaderEpochs() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        int recordsInSegment = 20;
        int numSegments = 10;
        for (int i = 0; i < numSegments; i++) {
            int offset = recordsInSegment * i;
            LogSegment segment = createLogSegment(offset);
            appendRecordBatch(segment, offset, 100, recordsInSegment);
            segment.onBecomeInactiveSegment();
            remoteStorageManager.copyLogSegment(TP0, segment, 0);
            remoteStorageManager.copyLogSegment(TP0, segment, 1);
        }
        List<RemoteLogSegmentInfo> allRemoteSegments = remoteStorageManager.listRemoteSegments(TP0);
        assertEquals(numSegments, allRemoteSegments.size());
        for (int i = 0; i < numSegments; i++) {
            RemoteLogSegmentInfo segment = allRemoteSegments.get(i);
            assertEquals(recordsInSegment * i, segment.baseOffset());
            assertEquals(recordsInSegment * (i + 1) - 1, segment.lastOffset());
            assertEquals(1, segment.leaderEpoch());
        }
    }

    @Test
    public void testListRemoteSegmentsDoesNotListSegmentsWithoutLogFile() throws IOException {
        remoteStorageManager.configure(basicProps(bucket));

        int leaderEpoch = 0;
        int recordsInSegment = 10;
        int numSegments = 2;
        for (int i = 0; i < numSegments; i++) {
            int offset = recordsInSegment * i;
            LogSegment segment = createLogSegment(offset);
            appendRecordBatch(segment, offset, 100, recordsInSegment);
            segment.onBecomeInactiveSegment();
            remoteStorageManager.copyLogSegment(TP0, segment, leaderEpoch);
        }

        deleteLogFile(0, 9, leaderEpoch);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            1 * recordsInSegment, recordsInSegment * 2 - 1, TP0, leaderEpoch, Collections.emptyMap());
        assertThat(
            remoteStorageManager.listRemoteSegments(TP0),
            containsInAnyOrder(segmentInfo)
        );
    }

    @Test
    public void testDeleteTopicPartition() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        remoteStorageManager.deleteTopicPartition(segmentOnS3Setup.topicPartition);
        assertThat(listS3Keys(), empty());
    }

    @Test
    public void testGetRemoteLogIndexEntries() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);
        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.topicPartition, 0,
            Collections.emptyMap());
        List<RemoteLogIndexEntry> result = remoteStorageManager.getRemoteLogIndexEntries(segmentInfo);
        assertEquals(segmentOnS3Setup.remoteLogIndexEntries, result);
    }

    @Test
    public void testGetRemoteLogIndexEntriesWithoutLogFile() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        deleteLogFile(segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.topicPartition, 0,
            Collections.emptyMap());
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.getRemoteLogIndexEntries(segmentInfo));
        assertEquals("Log file for " + segmentInfo + " doesn't exist", e.getMessage());
    }

    @Test
    public void testGetRemoteLogIndexEntriesNoIndexFile() throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        deleteRemoteLogIndexFile(segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch);

        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(
            segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.topicPartition, 0,
            Collections.emptyMap());
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.getRemoteLogIndexEntries(segmentInfo));
        assertEquals(
            "Error reading remote log index file " +
                s3Key(segmentOnS3Setup.topicPartition, "remote-log-index", segmentOnS3Setup.baseOffset, segmentOnS3Setup.lastOffset, segmentOnS3Setup.leaderEpoch),
            e.getMessage());
    }

    @Test
    public void testGetRemoteLogIndexEntriesCorruptedIndexFile() {
        // TODO implement
//        throw new RuntimeException("not implemented");
    }

    private void testMaxBytesLimitOneCompleteBatch(boolean minOneRecord) throws IOException {
        SegmentOnS3Setup segmentOnS3Setup = uploadSegment(0, 0, true);

        int maxBytes = segmentOnS3Setup.batchSizesInBytes.get(0) + 1;
        Records readRecords = remoteStorageManager.read(
            segmentOnS3Setup.remoteLogIndexEntries.get(0),
            maxBytes, 0, minOneRecord);

        assertEquals(NORMAL_BATCH_RECORD_COUNT, countRecords(readRecords));
        checkNormalBatch(readRecords, 0, 0);
        assertThat(readRecords.sizeInBytes(), lessThanOrEqualTo(maxBytes));
    }

    private SegmentOnS3Setup uploadSegment(int leaderEpoch, int segmentOffset, boolean configureRemoteStorageManager) throws IOException {
        LogSegment segment = createLogSegment(segmentOffset);
        appendRecordBatch(segment, segmentOffset + 0, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 10, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 20, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, segmentOffset + 21);
        appendRecordBatch(segment, segmentOffset + 31, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 41, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 51, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, segmentOffset + 52);
        appendRecordBatch(segment, segmentOffset + 62, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 72, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendRecordBatch(segment, segmentOffset + 82, NORMAL_RECORD_SIZE, NORMAL_BATCH_RECORD_COUNT);
        appendControlBatch(segment, segmentOffset + 83);
        segment.onBecomeInactiveSegment();

        if (segmentFileLastModified == null) {
            segmentFileLastModified = segment.lastModified();
        } else {
            segmentFileLastModified += 1000;
            segment.lastModified_$eq(segmentFileLastModified);
        }

        List<Integer> batchSizesInByte = StreamSupport.stream(segment.log().batches().spliterator(), false)
            .map(FileLogInputStream.FileChannelRecordBatch::sizeInBytes)
            .collect(Collectors.toList());
        if (configureRemoteStorageManager) {
            Map<String, String> props = basicProps(bucket);
            props.put(S3RemoteStorageManagerConfig.INDEX_INTERVAL_BYTES_CONFIG,
                Integer.toString(batchSizesInByte.get(0) + batchSizesInByte.get(1) + batchSizesInByte.get(2) + batchSizesInByte.get(3)));
            remoteStorageManager.configure(props);
        }

        List<RemoteLogIndexEntry> logIndexEntries = remoteStorageManager.copyLogSegment(TP0, segment, leaderEpoch);
        return new SegmentOnS3Setup(TP0, leaderEpoch, segment,
            logIndexEntries, batchSizesInByte, segmentOffset + 0, segmentOffset + 83);
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

    private void deleteLogFile(long baseOffset, long lastOffset, int leaderEpoch) {
        assert baseOffset <= lastOffset;
        s3Client.deleteObject(bucket, s3Key(TP0, "log", baseOffset, lastOffset, leaderEpoch));
    }

    private void deleteRemoteLogIndexFile(long baseOffset, long lastOffset, int leaderEpoch) {
        assert baseOffset <= lastOffset;
        s3Client.deleteObject(bucket, s3Key(TP0, "remote-log-index", baseOffset, lastOffset, leaderEpoch));
    }

    private List<String> listS3Keys() {
        List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket).getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private static class SegmentOnS3Setup {
        final TopicPartition topicPartition;
        final int leaderEpoch;
        final LogSegment segment;
        final List<RemoteLogIndexEntry> remoteLogIndexEntries;
        final List<Integer> batchSizesInBytes;
        final long baseOffset;
        final long lastOffset;

        SegmentOnS3Setup(TopicPartition topicPartition,
                         int leaderEpoch,
                         LogSegment segment,
                         List<RemoteLogIndexEntry> remoteLogIndexEntries,
                         List<Integer> batchSizesInBytes,
                         long baseOffset, long lastOffset) {
            this.topicPartition = topicPartition;
            this.leaderEpoch = leaderEpoch;
            this.segment = segment;
            this.remoteLogIndexEntries = new ArrayList<>(remoteLogIndexEntries);
            this.batchSizesInBytes = new ArrayList<>(batchSizesInBytes);
            this.baseOffset = baseOffset;
            this.lastOffset = lastOffset;
        }
    }
}

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
import java.util.concurrent.ExecutorService;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.internal.UploadCallable;
import com.amazonaws.services.s3.transfer.internal.UploadImpl;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate()
@PowerMockIgnore({"javax.*"})
public class S3RemoteStorageManagerWithoutS3Test extends S3RemoteStorageManagerTestBase {

    private S3RemoteStorageManager remoteStorageManager;

    @Before
    public void setUp() {
        Map<String, String> props = basicProps("does-not-matter");
        props.put(S3RemoteStorageManagerConfig.INDEX_INTERVAL_BYTES_CONFIG, "1");
        remoteStorageManager = new S3RemoteStorageManager(
            new AwsClientBuilder.EndpointConfiguration("https://does-not-matter.local:123", "eu-central-1"),
            null
        );
        remoteStorageManager.configure(props);
    }

    @After
    public void tearDown() {
        if (remoteStorageManager != null) {
            remoteStorageManager.close();
        }
    }

    @Test
    @PrepareForTest({TransferManager.class})
    public void testCopyCancellation() throws Exception {
        // Mock UploadCallable that doesn't do anything, just blocks.
        // This is needed to imitate long-running upload that can be reliably interrupted.
        PowerMock.expectNew(UploadCallable.class,
            EasyMock.anyObject(TransferManager.class),
            EasyMock.anyObject(ExecutorService.class),
            EasyMock.anyObject(UploadImpl.class),
            EasyMock.anyObject(PutObjectRequest.class),
            EasyMock.anyObject(ProgressListenerChain.class),
            EasyMock.anyString(),
            EasyMock.anyObject(TransferProgress.class))
            .andStubAnswer(new IAnswer<UploadCallable>() {
                @Override
                public UploadCallable answer() {
                    return new UploadCallable(
                        (TransferManager) EasyMock.getCurrentArguments()[0],
                        (ExecutorService) EasyMock.getCurrentArguments()[1],
                        (UploadImpl) EasyMock.getCurrentArguments()[2],
                        (PutObjectRequest) EasyMock.getCurrentArguments()[3],
                        (ProgressListenerChain) EasyMock.getCurrentArguments()[4],
                        (String) EasyMock.getCurrentArguments()[5],
                        (TransferProgress) EasyMock.getCurrentArguments()[6]) {
                        @Override
                        public UploadResult call() throws Exception {
                            Thread.sleep(10000);
                            throw new AssertionError("Shouldn't be here");
                        }
                    };
                }
            });
        PowerMock.replayAll();

        LogSegment segment1 = createLogSegment(0);
        appendRecordBatch(segment1, 0, 10, 10);
        segment1.onBecomeInactiveSegment();

        // Interrupt the upload concurrently.
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                remoteStorageManager.cancelCopyingLogSegment(TP0);
            } catch (InterruptedException ignored) { }
        }).start();
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.copyLogSegment(TP0, segment1, 0));
        assertEquals("Copying of segment " + segment1 + " for topic-partition " + TP0 + " interrupted", e.getMessage());
    }

    @Test
    public void testCopyLogSegmentWithUnreachableEndpoint() {
        LogSegment segment = createLogSegment(0);
        appendRecordBatch(segment, 0, 10, 10);
        segment.onBecomeInactiveSegment();

        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.copyLogSegment(TP0, segment, 0));
        assertEquals(e.getMessage(), "Error copying files for " + segment + " in " + TP0);
    }

    @Test
    public void testListRemoteSegmentsWithUnreachableEndpoint() {
        Throwable e = assertThrows(KafkaException.class, () -> remoteStorageManager.listRemoteSegments(TP0));
        assertEquals(e.getMessage(), "Error listing remote segments in " + TP0 + " with min base offset 0");
    }

    @Test
    public void testGetRemoteLogIndexEntriesWithUnreachableEndpoint() {
//        RemoteLogSegmentInfo segmentInfo = new RemoteLogSegmentInfo(0, 10, TP0, Collections.emptyMap());
//        Throwable e = assertThrows(KafkaException.class,
//            () -> remoteStorageManager.getRemoteLogIndexEntries(segmentInfo));
//        assertEquals(e.getMessage(), "Error checking marker file " + s3Key(TP0, "marker", 0, 10));
        throw new RuntimeException("rewrite w.r.t leader epoch");
    }

    @Test
    public void testReadWithUnreachableEndpoint() {
        String rdiStr = s3Key(TP0, "log", 0, 10, 0) + "#123";
        RemoteLogIndexEntry remoteLogIndexEntry = RemoteLogIndexEntry.apply(0, 10, 0, 100, 10, rdiStr.getBytes());
        Throwable e = assertThrows(KafkaException.class,
            () -> remoteStorageManager.read(remoteLogIndexEntry, Integer.MAX_VALUE, 0, false));
        assertEquals(e.getMessage(), "Error reading log file " + s3Key(TP0, "log", 0, 10, 0));
    }

    @Test
    public void testReadIncorrectRDI() {
        String rdiStr = s3Key(TP0, "log", 0, 10, 0) + "#incorrect";
        RemoteLogIndexEntry remoteLogIndexEntry = RemoteLogIndexEntry.apply(0, 10, 0, 100, 10, rdiStr.getBytes());
        Throwable e = assertThrows(IllegalArgumentException.class,
            () -> remoteStorageManager.read(remoteLogIndexEntry, Integer.MAX_VALUE, 0, false));
        assertEquals(e.getMessage(), "Can't parse RDI: " + rdiStr);
    }

    @Test
    @PrepareForTest({S3RemoteStorageManager.class})
    public void testAlreadyOngoingCopying() throws Exception {
        PowerMock.expectNew(TopicPartitionUploading.class,
            EasyMock.anyObject(TopicPartition.class),
            EasyMock.anyObject(LogSegment.class),
            EasyMock.anyString(),
            EasyMock.anyObject(TransferManager.class),
            EasyMock.anyInt())
            .andStubAnswer(new IAnswer<TopicPartitionUploading>() {
                @Override
                public TopicPartitionUploading answer() {
                    return new TopicPartitionUploading(
                        (TopicPartition) EasyMock.getCurrentArguments()[1], (int) EasyMock.getCurrentArguments()[0],
                        (LogSegment) EasyMock.getCurrentArguments()[2],
                        (String) EasyMock.getCurrentArguments()[3],
                        (TransferManager) EasyMock.getCurrentArguments()[4],
                        (Integer) EasyMock.getCurrentArguments()[5]) {
                        @Override
                        public List<RemoteLogIndexEntry> upload() throws IOException {
                            try {
                                Thread.sleep(10000);
                                throw new AssertionError("Shouldn't be here");
                            } catch (InterruptedException ignored) { }
                            return null;
                        }
                    };
                }
            });
        PowerMock.replayAll();

        LogSegment segment = createLogSegment(0);
        appendRecordBatch(segment, 0, 10, 10);
        segment.onBecomeInactiveSegment();

        new Thread(() -> {
            try {
                remoteStorageManager.copyLogSegment(TP0, segment, 0);
                throw new AssertionError("Shouldn't be here");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
        Thread.sleep(1000);

        // Try to run copy concurrently.
        Throwable e = assertThrows(IllegalStateException.class, () -> remoteStorageManager.copyLogSegment(TP0, segment, 0));
        assertEquals("Already ongoing copying for " + TP0, e.getMessage());
    }
}

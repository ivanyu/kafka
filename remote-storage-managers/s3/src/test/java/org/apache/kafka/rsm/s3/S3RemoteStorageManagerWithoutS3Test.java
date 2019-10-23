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

import java.util.Map;

import org.apache.kafka.common.KafkaException;

import com.amazonaws.client.builder.AwsClientBuilder;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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
}

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;

import kafka.log.remote.RDI;
import kafka.log.remote.RemoteLogIndexEntry;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemoteLogIndexerTest {

    @Test
    public void testIndexBasic() {
        List<FileChannelRecordBatch> batches = new ArrayList<>();
        // 1st entry
        batches.add(createBatch(0, 3, 0, 5, 0, 2));
        batches.add(createBatch(3, 3, 6, 9, 3, 8));
        batches.add(createBatch(6, 3, 10, 11, 9, 14));

        // Both batches are smaller than maxBytesInEntry separately, but not together.
        // 2nd entry
        batches.add(createBatch(9, 3, 12, 12, 15, 20));

        // 3rd entry
        batches.add(createBatch(12, 8, 13, 16, 21, 25));

        // The batch is bigger than maxBytesInEntry
        // 4th entry
        batches.add(createBatch(20, 100, 17, 20, 26, 30));

        List<RemoteLogIndexEntry> result = RemoteLogIndexer.index(
            batches, 10,
            batch -> new RDI(Long.toString(batch.baseOffset()).getBytes(StandardCharsets.UTF_8))
        );
        assertEquals(4, result.size());

        assertEquals(0, result.get(0).firstOffset());
        assertEquals(11, result.get(0).lastOffset());
        assertEquals(0, result.get(0).firstTimeStamp());
        assertEquals(14, result.get(0).lastTimeStamp());

        assertEquals(12, result.get(1).firstOffset());
        assertEquals(12, result.get(1).lastOffset());
        assertEquals(15, result.get(1).firstTimeStamp());
        assertEquals(20, result.get(1).lastTimeStamp());

        assertEquals(13, result.get(2).firstOffset());
        assertEquals(16, result.get(2).lastOffset());
        assertEquals(21, result.get(2).firstTimeStamp());
        assertEquals(25, result.get(2).lastTimeStamp());

        assertEquals(17, result.get(3).firstOffset());
        assertEquals(20, result.get(3).lastOffset());
        assertEquals(26, result.get(3).firstTimeStamp());
        assertEquals(30, result.get(3).lastTimeStamp());

    }

    private FileChannelRecordBatch createBatch(int position, int sizeInBytes,
                                               long baseOffset, long lastOffset,
                                               long firstTimestamp, long maxTimestamp) {
        FileChannelRecordBatch batch = EasyMock.mock(FileChannelRecordBatch.class);
        EasyMock.expect(batch.position()).andStubReturn(position);
        EasyMock.expect(batch.sizeInBytes()).andStubReturn(sizeInBytes);
        EasyMock.expect(batch.baseOffset()).andStubReturn(baseOffset);
        EasyMock.expect(batch.lastOffset()).andStubReturn(lastOffset);
        EasyMock.expect(batch.firstTimestamp()).andStubReturn(firstTimestamp);
        EasyMock.expect(batch.maxTimestamp()).andStubReturn(maxTimestamp);
        EasyMock.replay(batch);
        return batch;
    }
}

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
package org.apache.kafka.rsm.s3.files;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LastModifiedReverseIndexEntryTest {
    @Test
    public void testParse() {
        LastModifiedReverseIndexEntry expected = new LastModifiedReverseIndexEntry(
                new TopicPartition("some-topic", 777), 1572407431L, 0L, 123L, 7
        );
        LastModifiedReverseIndexEntry parsed = LastModifiedReverseIndexEntry.parse(
                "some-topic-777/last-modified-reverse-index/" +
                        "00000000001572407431-00000000000000000000-00000000000000000123-le0000000007");
        assertEquals(expected, parsed);
    }
}

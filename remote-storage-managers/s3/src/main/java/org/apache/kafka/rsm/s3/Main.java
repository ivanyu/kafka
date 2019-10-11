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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.server.KafkaConfig;
import scala.collection.JavaConverters;

public class Main {
    public static void main(String[] args) throws IOException {
//        List<RemoteLogSegmentInfo> remoteLogSegmentInfos = new ArrayList<>();
//        remoteLogSegmentInfos.add(new RemoteLogSegmentInfo() {
//            @Override
//            public long baseOffset() {
//                return 0;
//            }
//
//            @Override
//            public long endOffset() {
//                return 9;
//            }
//        });
//        remoteLogSegmentInfos.add(new RemoteLogSegmentInfo() {
//            @Override
//            public long baseOffset() {
//                return 10;
//            }
//
//            @Override
//            public long endOffset() {
//                return 19;
//            }
//        });
//
//        long offset = 5;
//
//        int index = Collections.binarySearch(remoteLogSegmentInfos, offset, new Comparator<Object>() {
////            override def compare(o1: Any, o2: Any): Int = {
////                java.lang.Long.compare(o1.asInstanceOf[RemoteLogSegmentInfo].baseOffset, o2.asInstanceOf[RemoteLogSegmentInfo].baseOffset)
////            }
//
//            @Override
//            public int compare(Object o1, Object o2) {
//                long o1Long;
//                if (o1 instanceof RemoteLogSegmentInfo) {
//                    o1Long = ((RemoteLogSegmentInfo)o1).baseOffset();
//                } else {
//                    o1Long = (long) o1;
//                }
//
//                long o2Long;
//                if (o2 instanceof RemoteLogSegmentInfo) {
//                    o2Long = ((RemoteLogSegmentInfo)o2).baseOffset();
//                } else {
//                    o2Long = (long) o2;
//                }
//
//                return java.lang.Long.compare(o1Long, o2Long);
//            }
//        });
//
//        System.out.println(index);

        S3RemoteStorageManager rsm = new S3RemoteStorageManager();

        Map<String, String> props = new HashMap<>();
        props.put(S3RemoteStorageManagerConfig.S3_BUCKET_NAME_CONFIG, "aiven-ivanyu-test");
        props.put(S3RemoteStorageManagerConfig.S3_REGION_CONFIG, "eu-central-1");
        rsm.configure(props);

        KafkaConfig kafkaConfig = KafkaConfig.fromProps(Utils.loadProps("/home/ivanyu/kafka/code/server.0.properties"));
        final Set<String> s = Collections.emptySet();
        LogConfig logConfig = new LogConfig(kafkaConfig.props(), JavaConverters.asScalaSet(s).toSet());

        Time time = Time.SYSTEM;

        RemoteLogIndexEntry firstRemoteLogIndexEntry = null;

        TopicPartition tp = new TopicPartition("test-topic", 0);
        int[] baseOffsets = new int[]{ 0, 96, 189, 192, 282, 375, 468, 561 };
        for (int i = 0; i < baseOffsets.length; i += 1) {
            LogSegment segment = LogSegment.open(
                new File("/home/ivanyu/kafka/code/run.0/test-topic-0"),
                baseOffsets[i],
                logConfig,
                time,
                true,
                0,
                false,
                ""
            );
            List<RemoteLogIndexEntry> remoteLog = rsm.copyLogSegment(tp, segment, 0);
            if (!remoteLog.isEmpty()) {
                System.out.println(remoteLog.get(0));
                if (firstRemoteLogIndexEntry == null) {
                    firstRemoteLogIndexEntry = remoteLog.get(0);
                }
            }

            if (i < 3) {
                rsm.copyLogSegment(tp, segment, 1);
            }
        }
        
        Records records = rsm.read(firstRemoteLogIndexEntry, 10000000, 4, true);
        System.out.println(records);

        List<RemoteLogSegmentInfo> remoteSegmentInfos = rsm.listRemoteSegments(tp);
        System.out.println(remoteSegmentInfos);

//        System.out.println(rsm.deleteLogSegment(new RemoteLogSegmentInfo(0, 6, tp, Collections.emptyMap())));
//        System.out.println(rsm.deleteLogSegment(remoteSegmentInfos.get(0)));
//        System.out.println(rsm.deleteLogSegment(remoteSegmentInfos.get(0)));
//
//        for (int i = 1; i < baseOffsets.length; i += 1) {
//            rsm.deleteLogSegment(remoteSegmentInfos.get(i));
//        }

        rsm.close();
    }
}

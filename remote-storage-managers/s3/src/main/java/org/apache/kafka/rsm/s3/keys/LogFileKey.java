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
package org.apache.kafka.rsm.s3.keys;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

public class LogFileKey extends Key {
    private static final String DIRECTORY = "log";

    private static final Pattern TOPIC_PARTITION_PATTERN = Pattern.compile("([^/])+-(\\d)+?/.*");

    private LogFileKey() {}

    public static String key(TopicPartition topicPartition, long baseOffset, long lastOffset, int leaderEpoch) {
        return keyPrefixWithoutLeaderEpochNumber(topicPartition, baseOffset, lastOffset) + formalInteger(leaderEpoch);
    }

    public static String keyPrefixWithoutLeaderEpochNumber(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return baseOffsetPrefix(topicPartition, baseOffset) + "-" + formatLong(lastOffset) + "-le";
    }

    public static String baseOffsetPrefix(TopicPartition topicPartition, long baseOffset) {
        return directoryPrefix(topicPartition) + formatLong(baseOffset);
    }

    public static String directoryPrefix(TopicPartition topicPartition) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR;
    }

    public static TopicPartition topicPartition(String s3Key) {
        Matcher matcher = TOPIC_PARTITION_PATTERN.matcher(s3Key);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Can't extract topic-partition from S3 key " + s3Key);
        }

        String topic = matcher.group(1);
        int partition = Integer.parseInt(matcher.group(2));
        return new TopicPartition(topic, partition);
    }
}

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class to work with S3 keys for Kafka log files.
 */
public class LogFileS3Key extends S3Key {
    private static final String DIRECTORY = "log";
    private static final Pattern NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})-le(\\d{10})");

    private LogFileS3Key() {}

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

    public static SegmentInfo parse(String s3Key) {
        TopicPartition topicPartition = topicPartition(s3Key);

        int iSep = s3Key.indexOf(DIRECTORY_SEPARATOR);
        if (iSep == -1) {
            throw new IllegalArgumentException("Invalid log file S3 key format: " + s3Key);
        }

        String remaining = s3Key.substring(iSep);
        if (!remaining.startsWith(DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR)) {
            throw new IllegalArgumentException("Invalid log file S3 key format: " + s3Key);
        }

        remaining = remaining.substring((DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR).length());

        Matcher m = NAME_PATTERN.matcher(remaining);
        if (m.matches()) {
            long baseOffset = Long.parseLong(m.group(1));
            long lastOffset = Long.parseLong(m.group(2));
            int leaderEpoch = Integer.parseInt(m.group(3));
            return new SegmentInfo(topicPartition, baseOffset, lastOffset, leaderEpoch);
        } else {
            throw new IllegalArgumentException("Invalid log file S3 key format: " + s3Key);
        }
    }
}

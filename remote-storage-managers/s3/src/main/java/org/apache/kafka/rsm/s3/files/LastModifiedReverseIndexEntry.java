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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A last modified reverse index entry.
 *
 * <p>Mostly a utility class to work with S3 keys that represent index entries.
 */
public class LastModifiedReverseIndexEntry extends S3Key {
    private static final String DIRECTORY = "last-modified-reverse-index";
    private static final Pattern NAME_PATTERN = Pattern.compile(
        "(?<lastModifiedMs>\\d{20})-(?<lastOffset>\\d{20})-(?<baseOffset>\\d{20})-le(?<leaderEpoch>\\d{10})");

    private final TopicPartition topicPartition;
    private final long lastModifiedMs;
    private final long baseOffset;
    private final long lastOffset;
    private final int leaderEpoch;

    LastModifiedReverseIndexEntry(TopicPartition topicPartition,
                                  long lastModifiedMs,
                                  long baseOffset,
                                  long lastOffset,
                                  int leaderEpoch) {
        this.topicPartition = topicPartition;
        this.lastModifiedMs = lastModifiedMs;
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.leaderEpoch = leaderEpoch;
    }

    public long lastModifiedMs() {
        return lastModifiedMs;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LastModifiedReverseIndexEntry that = (LastModifiedReverseIndexEntry) o;
        return lastModifiedMs == that.lastModifiedMs &&
                baseOffset == that.baseOffset &&
                lastOffset == that.lastOffset &&
                leaderEpoch == that.leaderEpoch &&
                Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, lastModifiedMs, baseOffset, lastOffset, leaderEpoch);
    }

    public static String key(TopicPartition topicPartition, long lastModifiedMs, long baseOffset, long lastOffset, int leaderEpoch) {
        return directoryPrefix(topicPartition)
                + formatLong(lastModifiedMs) + "-"
                + formatLong(lastOffset) + "-"
                + formatLong(baseOffset) + "-le"
                + formalInteger(leaderEpoch);
    }

    public static String directoryPrefix(TopicPartition topicPartition) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR;
    }

    public static LastModifiedReverseIndexEntry parse(String s3Key) {
        TopicPartition topicPartition = topicPartition(s3Key);

        int iSep = s3Key.indexOf(DIRECTORY_SEPARATOR);
        if (iSep == -1) {
            throw new IllegalArgumentException("Invalid last modified reverse index entry format: " + s3Key);
        }

        String remaining = s3Key.substring(iSep);
        if (!remaining.startsWith(DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR)) {
            throw new IllegalArgumentException("Invalid last modified reverse index entry format: " + s3Key);
        }

        remaining = remaining.substring((DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR).length());

        Matcher m = NAME_PATTERN.matcher(remaining);
        if (m.matches()) {
            long lastModifiedMs = Long.parseLong(m.group("lastModifiedMs"));
            long baseOffset = Long.parseLong(m.group("baseOffset"));
            long lastOffset = Long.parseLong(m.group("lastOffset"));
            int leaderEpoch = Integer.parseInt(m.group("leaderEpoch"));
            return new LastModifiedReverseIndexEntry(topicPartition, lastModifiedMs, baseOffset, lastOffset, leaderEpoch);
        } else {
            throw new IllegalArgumentException("Invalid last modified reverse index entry format: " + s3Key);
        }
    }
}

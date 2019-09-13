package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

public final class MarkerKey extends Key {
    private static final String DIRECTORY = "marker";

    private MarkerKey() {}

    public static String key(TopicPartition topicPartition, long baseOffset, long lastOffset, int leaderEpoch) {
        return keyPrefixWithoutLeaderEpochNumber(topicPartition, baseOffset, lastOffset) + leaderEpoch;
    }

    public static String keyPrefixWithoutLeaderEpochNumber(TopicPartition topicPartition, long baseOffset, long lastOffset) {
        return baseOffsetPrefix(topicPartition, baseOffset) + "-" + formatLong(lastOffset) + "-le";
    }

    public static String baseOffsetPrefix(TopicPartition topicPartition, long baseOffset) {
        return directoryPrefix(topicPartition) + formatLong(baseOffset);
    }

    public static String directoryPrefix(TopicPartition topicPartition) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY + "/";
    }
}

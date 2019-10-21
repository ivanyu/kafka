package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

public class LastModifiedReverseIndexKey extends Key {
    private static final String DIRECTORY = "last-modified-reverse-index";

    private LastModifiedReverseIndexKey() {}

    public static String key(TopicPartition topicPartition, long lastModifiedMs, long baseOffset, long lastOffset, int leaderEpoch) {
        return directoryPrefix(topicPartition)
                + formatLong(lastModifiedMs) + "-"
                + formatLong(baseOffset) + "-"
                + formatLong(lastOffset) + "-le"
                + formalInteger(leaderEpoch);
    }

    public static String directoryPrefix(TopicPartition topicPartition) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR;
    }
}

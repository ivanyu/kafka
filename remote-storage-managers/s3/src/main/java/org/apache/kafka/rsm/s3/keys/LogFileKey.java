package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

public class LogFileKey extends Key {
    private static final String DIRECTORY = "log";

    private LogFileKey() {}

    public static String key(TopicPartition topicPartition, long baseOffset, long lastOffset, int leaderEpoch) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY + "/"
            + formatLong(baseOffset)
            + "-"
            + formatLong(lastOffset)
            + "-le" + leaderEpoch;
    }
}

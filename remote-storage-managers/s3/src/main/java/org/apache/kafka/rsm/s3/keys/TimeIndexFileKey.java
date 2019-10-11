package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

public class TimeIndexFileKey extends Key {
    private static final String DIRECTORY = "time-index";

    private TimeIndexFileKey() {}

    public static String key(TopicPartition topicPartition, long baseOffset, long lastOffset, int leaderEpoch) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR
            + formatLong(baseOffset)
            + "-"
            + formatLong(lastOffset)
            + "-le" + leaderEpoch;
    }
}

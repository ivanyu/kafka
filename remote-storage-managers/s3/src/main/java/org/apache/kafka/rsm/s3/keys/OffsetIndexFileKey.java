package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

public class OffsetIndexFileKey extends Key {
    private static final String DIRECTORY = "index";

    private OffsetIndexFileKey() {}

    public static String key(TopicPartition topicPartition, long baseOffset, long lastOffset, int leaderEpoch) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR
            + formatLong(baseOffset)
            + "-"
            + formatLong(lastOffset)
            + "-le" + formalInteger(leaderEpoch);
    }
}

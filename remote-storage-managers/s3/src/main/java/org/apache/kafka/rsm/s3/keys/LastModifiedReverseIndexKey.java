package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

import kafka.log.Log;

public class LastModifiedReverseIndexKey extends Key {
    private static final String DIRECTORY = "last-modified-reverse-index";

    private LastModifiedReverseIndexKey() {}

    public static String key(TopicPartition topicPartition, long lastModifiedMs, long baseOffset, long lastOffset, int leaderEpoch) {
        return directoryPrefix(topicPartition) +
            // Offset formatting (20 digits) is fine for timestamps too.
            Log.filenamePrefixFromOffset(lastModifiedMs) + "-" +
            Log.filenamePrefixFromOffset(baseOffset) + "-" +
            Log.filenamePrefixFromOffset(lastOffset) + "-le" + leaderEpoch;
    }

    public static String directoryPrefix(TopicPartition topicPartition) {
        return topicPartitionDirectory(topicPartition) + DIRECTORY_SEPARATOR + DIRECTORY + DIRECTORY_SEPARATOR;
    }
}

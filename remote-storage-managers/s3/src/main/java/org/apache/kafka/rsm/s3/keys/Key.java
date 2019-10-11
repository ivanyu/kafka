package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

import kafka.log.Log;

abstract class Key {
    static final String DIRECTORY_SEPARATOR = "/";

    static String formatLong(long value) {
        return Log.filenamePrefixFromOffset(value);
    }

    static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString();
    }
}

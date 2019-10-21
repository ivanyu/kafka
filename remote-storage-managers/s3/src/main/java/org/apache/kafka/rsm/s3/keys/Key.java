package org.apache.kafka.rsm.s3.keys;

import org.apache.kafka.common.TopicPartition;

import kafka.log.Log;

import java.text.NumberFormat;

abstract class Key {
    static final String DIRECTORY_SEPARATOR = "/";

    private static final NumberFormat INTEGER_FORMAT = NumberFormat.getInstance();
    static {
        INTEGER_FORMAT.setMinimumIntegerDigits(10);
        INTEGER_FORMAT.setMaximumFractionDigits(0);
        INTEGER_FORMAT.setGroupingUsed(false);
    }

    static String formalInteger(int value) {
        return INTEGER_FORMAT.format(value);
    }

    static String formatLong(long value) {
        return Log.filenamePrefixFromOffset(value);
    }

    static String topicPartitionDirectory(TopicPartition topicPartition) {
        return topicPartition.toString();
    }
}

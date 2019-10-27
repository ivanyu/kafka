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

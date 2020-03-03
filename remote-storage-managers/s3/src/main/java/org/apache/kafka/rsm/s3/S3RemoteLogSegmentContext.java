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
package org.apache.kafka.rsm.s3;

import java.nio.ByteBuffer;

import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentContext;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

public class S3RemoteLogSegmentContext implements RemoteLogSegmentContext {

    public static final String LOG_FILE_NAME_KEY_NAME = "log_file_name";
    public static final String OFFSET_INDEX_FILE_NAME_KEY_NAME = "offset_index_file_name";
    public static final String TIME_INDEX_FILE_NAME_KEY_NAME = "time_index_file_name";
    public static final Schema SCHEMA = new Schema(
        new Field(LOG_FILE_NAME_KEY_NAME, Type.STRING),
        new Field(OFFSET_INDEX_FILE_NAME_KEY_NAME, Type.STRING),
        new Field(TIME_INDEX_FILE_NAME_KEY_NAME, Type.STRING)
    );

    private final String logFileName;
    private final String offsetIndexFileName;
    private final String timeIndexFileName;

    public S3RemoteLogSegmentContext(final String logFileName,
                                     final String offsetIndexFileName,
                                     final String timeIndexFileName) {
        this.logFileName = logFileName;
        this.offsetIndexFileName = offsetIndexFileName;
        this.timeIndexFileName = timeIndexFileName;
    }

    public String logFileName() {
        return logFileName;
    }

    public String offsetIndexFileName() {
        return offsetIndexFileName;
    }

    public String timeIndexFileName() {
        return timeIndexFileName;
    }

    @Override
    public byte[] asBytes() {
        final Struct struct = new Struct(SCHEMA);
        struct.set(LOG_FILE_NAME_KEY_NAME, logFileName);
        struct.set(LOG_FILE_NAME_KEY_NAME, logFileName);
        struct.set(OFFSET_INDEX_FILE_NAME_KEY_NAME, offsetIndexFileName);
        struct.set(TIME_INDEX_FILE_NAME_KEY_NAME, timeIndexFileName);
        final ByteBuffer buf = ByteBuffer.allocate(SCHEMA.sizeOf(struct));
        struct.writeTo(buf);
        return buf.array();
    }

    public static S3RemoteLogSegmentContext fromBytes(final byte[] bytes) {
        final Struct struct = SCHEMA.read(ByteBuffer.wrap(bytes));
        return new S3RemoteLogSegmentContext(
            struct.getString(LOG_FILE_NAME_KEY_NAME),
            struct.getString(OFFSET_INDEX_FILE_NAME_KEY_NAME),
            struct.getString(TIME_INDEX_FILE_NAME_KEY_NAME)
        );
    }
}

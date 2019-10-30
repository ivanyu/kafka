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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.utils.ByteBufferInputStream;

/**
 * An {@link InputStream} backed by multiple {@link ByteBuffer}s.
 *
 * <p>The class is similar to {@link ByteBufferInputStream}, but allows to read
 * from multiple byte buffers as if they were one without copying.
 */
class GatheringByteBufferInputStream extends InputStream {
    // No need to close ByteBufferInputStream.
    private final LinkedList<ByteBufferInputStream> inputStreams;

    GatheringByteBufferInputStream(List<ByteBuffer> inputStreams) {
        if (inputStreams == null) {
            throw new IllegalArgumentException("inputStreams can't be null");
        }

        this.inputStreams = new LinkedList<>();
        for (ByteBuffer buffer : inputStreams) {
            this.inputStreams.add(new ByteBufferInputStream(buffer));
        }
    }

    @Override
    public int read() throws IOException {
        while (!inputStreams.isEmpty()) {
            int result = inputStreams.getFirst().read();
            if (result > -1) {
                return result;
            } else {
                inputStreams.removeFirst();
            }
        }
        return -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        int totalReadBytes = -1;
        int remainToRead = len;
        while (!inputStreams.isEmpty() && remainToRead > 0) {
            int offset = Math.max(0, totalReadBytes);
            int readBytes = inputStreams.getFirst().read(bytes, offset, remainToRead);
            if (readBytes > -1) {
                if (totalReadBytes == -1) {
                    totalReadBytes = 0;
                }
                remainToRead -= readBytes;
                totalReadBytes += readBytes;
            } else {
                inputStreams.removeFirst();
            }
        }
        return totalReadBytes;
    }
}

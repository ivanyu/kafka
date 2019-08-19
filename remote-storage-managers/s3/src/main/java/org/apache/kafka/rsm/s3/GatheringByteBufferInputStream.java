package org.apache.kafka.rsm.s3;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.utils.ByteBufferInputStream;

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

package org.apache.kafka.rsm.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class GatheringByteBufferInputStreamTest {
    @Test
    public void testNullInput() {
        Throwable t = assertThrows(IllegalArgumentException.class, () -> new GatheringByteBufferInputStream(null));
        assertEquals("inputStreams can't be null", t.getMessage());
    }

    @Test
    public void testEmptyInputSingleByteRead() throws IOException {
        GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(Collections.emptyList());
        assertEquals(-1, is.read());
    }

    @Test
    public void testEmptyInputArrayRead() throws IOException {
        GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(Collections.emptyList());
        byte[] bytes = new byte[100];
        assertEquals(-1, is.read(bytes, 0, 100));
    }

    @Test
    public void testSingleEmptyBufferSingleByteRead() throws IOException {
        List<ByteBuffer> buffers = Collections.singletonList(ByteBuffer.allocate(0));
        GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(buffers);
        assertEquals(-1, is.read());
    }

    @Test
    public void testSingleEmptyBufferArrayRead() throws IOException {
        List<ByteBuffer> buffers = Collections.singletonList(ByteBuffer.allocate(0));
        GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(buffers);
        byte[] bytes = new byte[100];
        assertEquals(-1, is.read(bytes, 0, 100));
    }

    @Test
    public void testMultipleByteBuffers() throws IOException {
        List<ByteBuffer> buffers = Arrays.asList(
            ByteBuffer.allocate(10),
            ByteBuffer.allocate(1000),
            ByteBuffer.allocate(50),
            ByteBuffer.allocate(0),
            ByteBuffer.allocate(100)
        );
        int totalCapacity = 0;
        for (int bufI = 0; bufI < buffers.size(); bufI++) {
            ByteBuffer buffer = buffers.get(bufI);
            totalCapacity += buffer.capacity();
            for (int j = 0; j < buffer.capacity(); j++) {
                buffer.put((byte) bufI);
            }
            buffer.flip();
        }

        ByteBuffer expectedBuffer = ByteBuffer.allocate(totalCapacity);
        for (int bufI = 0; bufI < buffers.size(); bufI++) {
            ByteBuffer buffer = buffers.get(bufI);
            expectedBuffer.put(buffer);
            buffer.flip();
        }

        // Single byte reads.
        {
            buffers.forEach(b -> b.position(0));
            expectedBuffer.flip();
            ByteBuffer readBuffer = ByteBuffer.allocate(totalCapacity);
            GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(buffers);
            for (int i = 0; i < totalCapacity; i++) {
                int result = is.read();
                assert result >= 0;
                readBuffer.put((byte) result);
            }
            assertEquals(-1, is.read());

            readBuffer.flip();
            assertArrayEquals(expectedBuffer.array(), readBuffer.array());
        }

        // Array reads in small chunks.
        {
            buffers.forEach(b -> b.position(0));
            expectedBuffer.flip();
            ByteBuffer readBuffer = ByteBuffer.allocate(totalCapacity);
            GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(buffers);
            int remainRead = totalCapacity;
            byte[] bytes = new byte[7];
            while (remainRead > 0) {
                int result = is.read(bytes, 0, bytes.length);
                assert result >= 0;
                readBuffer.put(bytes, 0, result);
                remainRead -= result;
            }
            readBuffer.flip();
            assertArrayEquals(expectedBuffer.array(), readBuffer.array());
        }

        // Read using ReadableByteChannel.
        {
            buffers.forEach(b -> b.position(0));
            expectedBuffer.flip();
            ByteBuffer readBuffer = ByteBuffer.allocate(totalCapacity);
            GatheringByteBufferInputStream is = new GatheringByteBufferInputStream(buffers);
            Channels.newChannel(is).read(readBuffer);
            readBuffer.flip();
            assertArrayEquals(expectedBuffer.array(), readBuffer.array());
        }
    }
}

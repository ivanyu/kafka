package org.apache.kafka.rsm.s3;

import java.util.Objects;

final class OffsetPair {
    private final long baseOffset;
    private final long lastOffset;

        OffsetPair(long baseOffset, long lastOffset) {
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetPair that = (OffsetPair) o;
        return baseOffset == that.baseOffset &&
            lastOffset == that.lastOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseOffset, lastOffset);
    }
}

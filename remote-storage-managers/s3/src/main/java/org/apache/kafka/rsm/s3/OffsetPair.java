package org.apache.kafka.rsm.s3;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class OffsetPair {
    private final long baseOffset;
    private final long endOffset;

        OffsetPair(long baseOffset, long endOffset) {
        this.baseOffset = baseOffset;
        this.endOffset = endOffset;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long endOffset() {
        return endOffset;
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
            endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseOffset, endOffset);
    }
}

package org.apache.kafka.rsm.s3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LastModifiedReverseIndexEntry {
    private static final Pattern NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})-(\\d{20})-le(\\d+)");

    private final long lastModifiedMs;
    private final long baseOffset;
    private final long endOffset;
    private final int leaderEpoch;

    public LastModifiedReverseIndexEntry(long lastModifiedMs, long baseOffset, long endOffset, int leaderEpoch) {
        this.lastModifiedMs = lastModifiedMs;
        this.baseOffset = baseOffset;
        this.endOffset = endOffset;
        this.leaderEpoch = leaderEpoch;
    }

    public long lastModifiedMs() {
        return lastModifiedMs;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * Parses entry string in the format {@code {last modified ms}-{base offset}-{last offset}-le{leader epoch}}.
     * @throws IllegalArgumentException if the format is incorrect.
     */
    public static LastModifiedReverseIndexEntry parse(String entryStr) {
        Matcher m = NAME_PATTERN.matcher(entryStr);
        if (m.matches()) {
            long lastModifiedMs = Long.parseLong(m.group(1));
            long baseOffset = Long.parseLong(m.group(2));
            long lastOffset = Long.parseLong(m.group(3));
            int leaderEpoch = Integer.parseInt(m.group(4));
            return new LastModifiedReverseIndexEntry(lastModifiedMs, baseOffset, lastOffset, leaderEpoch);
        } else {
            throw new IllegalArgumentException("Invalid last modified reverse index entry format: " + entryStr);
        }
    }
}

package org.apache.kafka.rsm.s3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Marker {
    private static final Pattern NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})-le(\\d+)");

    private final long baseOffset;
    private final long lastOffset;
    private final int leaderEpoch;

    private final OffsetPair offsetPair;

    private Marker(long baseOffset, long lastOffset, int leaderEpoch) {
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.leaderEpoch = leaderEpoch;

        this.offsetPair = new OffsetPair(baseOffset, lastOffset);
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public OffsetPair offsetPair() {
        return offsetPair;
    }

    /**
     * Parses marker string in the format {@code {base offset}-{last offset}-le{leader epoch}}.
     * @throws IllegalArgumentException if the format is incorrect.
     */
    public static Marker parse(String markerStr) {
        Matcher m = NAME_PATTERN.matcher(markerStr);
        if (m.matches()) {
            long baseOffset = Long.parseLong(m.group(1));
            long lastOffset = Long.parseLong(m.group(2));
            int leaderEpoch = Integer.parseInt(m.group(3));
            return new Marker(baseOffset, lastOffset, leaderEpoch);
        } else {
            throw new IllegalArgumentException("Invalid marker format: " + markerStr);
        }
    }
}

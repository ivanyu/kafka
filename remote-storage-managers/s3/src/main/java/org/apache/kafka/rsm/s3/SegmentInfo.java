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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SegmentInfo {
    private static final Pattern NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})-le(\\d+)");

    private final long baseOffset;
    private final long lastOffset;
    private final int leaderEpoch;

    private final OffsetPair offsetPair;

    private SegmentInfo(long baseOffset, long lastOffset, int leaderEpoch) {
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
     * Parses a segment info string in the format {@code {base offset}-{last offset}-le{leader epoch}}.
     * @throws IllegalArgumentException if the format is incorrect.
     */
    public static SegmentInfo parse(String coordinatesStr) {
        Matcher m = NAME_PATTERN.matcher(coordinatesStr);
        if (m.matches()) {
            long baseOffset = Long.parseLong(m.group(1));
            long lastOffset = Long.parseLong(m.group(2));
            int leaderEpoch = Integer.parseInt(m.group(3));
            return new SegmentInfo(baseOffset, lastOffset, leaderEpoch);
        } else {
            throw new IllegalArgumentException("Invalid segment info format: " + coordinatesStr);
        }
    }
}

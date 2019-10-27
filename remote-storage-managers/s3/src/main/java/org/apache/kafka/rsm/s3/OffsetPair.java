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

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
package org.apache.kafka.common.log.remote.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Metadata about the log segment stored in remote tier storage.
 */
public class RemoteLogSegmentMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Start offset of this segment.
     */
    private final long startOffset;

    /**
     * End offset of this segment.
     */
    private final long endOffset;

    /**
     * Leader epoch of the broker.
     */
    private final int leaderEpoch;

    /**
     * Maximum timestamp in the segment
     */
    private final long maxTimestamp;

    /**
     * Epoch time at which the remote log segment is copied to the remote tier storage.
     */
    private long createdTimestamp;

    /**
     * It indicates that this is marked for deletion.
     */
    private boolean markedForDeletion;

    /**
     * Any context returned by {@link RemoteStorageManager#copyLogSegment(RemoteLogSegmentId, LogSegmentData)} for
     * the given remoteLogSegmentId
     */
    private final byte[] remoteLogSegmentContext;

    /**
     * @param remoteLogSegmentId      Universally unique remote log segment id.
     * @param startOffset             Start offset of this segment.
     * @param endOffset               End offset of this segment.
     * @param maxTimeStampSoFar
     * @param leaderEpoch             Leader epoch of the broker.
     * @param remoteLogSegmentContext Any context returned by {@link RemoteStorageManager#copyLogSegment(RemoteLogSegmentId, LogSegmentData)} for
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId, long startOffset, long endOffset,
                                    long maxTimeStampSoFar, int leaderEpoch, byte[] remoteLogSegmentContext) {
        this(remoteLogSegmentId,
                startOffset,
                endOffset,
                maxTimeStampSoFar,
                leaderEpoch,
                0,
                false,
                remoteLogSegmentContext);
    }

    /**
     * @param remoteLogSegmentId      Universally unique remote log segment id.
     * @param startOffset             Start offset of this segment.
     * @param endOffset               End offset of this segment.
     * @param maxTimestamp            maximum timestamp in this segment
     * @param leaderEpoch             Leader epoch of the broker.
     * @param createdTimestamp        Epoch time at which the remote log segment is copied to the remote tier storage.
     * @param markedForDeletion       The respective segment of remoteLogSegmentId is marked fro deletion.
     * @param remoteLogSegmentContext Any context returned by {@link RemoteStorageManager#copyLogSegment(RemoteLogSegmentId, LogSegmentData)}
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId, long startOffset, long endOffset,
                                    long maxTimestamp, int leaderEpoch, long createdTimestamp,
                                    boolean markedForDeletion, byte[] remoteLogSegmentContext) {
        this.remoteLogSegmentId = remoteLogSegmentId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.leaderEpoch = leaderEpoch;
        this.maxTimestamp = maxTimestamp;
        this.createdTimestamp = createdTimestamp;
        this.markedForDeletion = markedForDeletion;
        this.remoteLogSegmentContext = remoteLogSegmentContext;
    }

    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long createdTimestamp() {
        return createdTimestamp;
    }

    public boolean isCreated() {
        return createdTimestamp > 0;
    }

    public boolean markedForDeletion() {
        return markedForDeletion;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public byte[] remoteLogSegmentContext() {
        return remoteLogSegmentContext;
    }

    //todo Add efficient ser/des mechanism, may be avro/protobuf or any other.
    public static byte[] asBytes(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(remoteLogSegmentMetadata);
            return baos.toByteArray();
        }
    }

    public static RemoteLogSegmentMetadata fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (RemoteLogSegmentMetadata) objectInputStream.readObject();
        }
    }

    public static RemoteLogSegmentMetadata markForDeletion(RemoteLogSegmentMetadata original) {
        return new RemoteLogSegmentMetadata(original.remoteLogSegmentId, original.startOffset, original.endOffset,
                original.maxTimestamp, original.leaderEpoch, original.createdTimestamp, true,
                original.remoteLogSegmentContext);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadata{" +
                "remoteLogSegmentId=" + remoteLogSegmentId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", leaderEpoch=" + leaderEpoch +
                ", maxTimestamp=" + maxTimestamp +
                ", createdTimestamp=" + createdTimestamp +
                ", markedForDeletion=" + markedForDeletion +
                ", remoteLogSegmentContext=" + Arrays.toString(remoteLogSegmentContext) +
                '}';
    }
}
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;

import kafka.log.remote.RDI;
import kafka.log.remote.RemoteLogIndexEntry;

/**
 * Create {@link RemoteLogIndexEntry}s of {@link FileChannelRecordBatch}es;
 */
public class RemoteLogIndexer {
    private final int indexIntervalBytes;
    private final Function<FileChannelRecordBatch, RDI> createRDI;

    private final List<RemoteLogIndexEntry> result = new ArrayList<>();

    private FileChannelRecordBatch firstBatch = null;
    private FileChannelRecordBatch lastBatch = null;

    private RemoteLogIndexer(Iterable<FileChannelRecordBatch> recordBatches,
                            int indexIntervalBytes,
                            Function<FileChannelRecordBatch, RDI> createRDI) {
        this.indexIntervalBytes = indexIntervalBytes;
        this.createRDI = createRDI;

        for (FileChannelRecordBatch batch : recordBatches) {
            if (!isCurrentEntryExist()) {
                startNewEntry(batch);
            } else if (canAddToCurrentEntry(batch)) {
                lastBatch = batch;
            } else {
                finalizeCurrentEntry();
                startNewEntry(batch);
            }
        }

        if (isCurrentEntryExist()) {
            finalizeCurrentEntry();
        }
    }

    private boolean isCurrentEntryExist() {
        return firstBatch != null;
    }

    private void startNewEntry(FileChannelRecordBatch batch) {
        firstBatch = batch;
        lastBatch = batch;
    }

    private void finalizeCurrentEntry() {
        RemoteLogIndexEntry entry = RemoteLogIndexEntry.apply(
            firstBatch.baseOffset(),
            lastBatch.lastOffset(),
            firstBatch.firstTimestamp(),
            lastBatch.maxTimestamp(),
            bytesCovered(),
            createRDI.apply(firstBatch).value());
        result.add(entry);
    }

    private boolean canAddToCurrentEntry(FileChannelRecordBatch batch) {
        return bytesCovered() + batch.sizeInBytes() <= indexIntervalBytes;
    }

    private int bytesCovered() {
        return lastBatch.position() - firstBatch.position() + lastBatch.sizeInBytes();
    }

    /**
     * Index provided {@code recordBatches}.
     *
     * @param createRDI the function that takes the first batch in an index
     *                  entry and returns the {@link RDI} for it.
     * @return the list of {@link RemoteLogIndexEntry} for the provided {@code recordBatches}.
     */
    public static List<RemoteLogIndexEntry> index(Iterable<FileChannelRecordBatch> recordBatches,
                                                  int indexIntervalBytes,
                                                  Function<FileChannelRecordBatch, RDI> createRDI) {
        RemoteLogIndexer instance = new RemoteLogIndexer(recordBatches, indexIntervalBytes, createRDI);
        return instance.result;
    }
}

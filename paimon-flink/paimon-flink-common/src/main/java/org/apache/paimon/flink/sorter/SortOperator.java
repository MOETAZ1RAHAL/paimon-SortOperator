/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sorter;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

/** SortOperator to sort the `InternalRow`s by the `KeyType`. */
public class SortOperator extends TableStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow>, BoundedOneInput {

    private final int sinkParallelism;
    private final SortBufferManager sortBufferManager;

    public SortOperator(
            RowType keyType,
            RowType rowType,
            long maxMemory,
            int pageSize,
            int spillSortMaxNumFiles,
            String spillCompression,
            int sinkParallelism,
            MemorySize maxDiskSize) {
        this.sinkParallelism = sinkParallelism;
        this.sortBufferManager =
                new SortBufferManager(
                        keyType,
                        rowType,
                        maxMemory,
                        pageSize,
                        spillSortMaxNumFiles,
                        spillCompression,
                        maxDiskSize);
    }

    @Override
    public void open() throws Exception {
        super.open();
        sortBufferManager.open(
                getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths());
        if (sinkParallelism != getRuntimeContext().getNumberOfParallelSubtasks()) {
            throw new IllegalArgumentException(
                    "Please ensure that the runtime parallelism of the sink matches the initial configuration "
                            + "to avoid potential issues with skewed range partitioning.");
        }
    }

    @Override
    public void endInput() throws Exception {
        if (sortBufferManager.size() > 0) {
            MutableObjectIterator<BinaryRow> iterator = sortBufferManager.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(sortBufferManager.getArity());
            while ((binaryRow = iterator.next(binaryRow)) != null) {
                output.collect(new StreamRecord<>(binaryRow));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        sortBufferManager.close();
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        sortBufferManager.write(element.getValue());
    }

    @VisibleForTesting
    BinaryExternalSortBuffer getBuffer() {
        return sortBufferManager.getBuffer();
    }
}

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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

/**
 * Manager for {@link BinaryExternalSortBuffer} and {@link IOManager} to sort the {@link
 * InternalRow}s.
 */
public class SortBufferManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType keyType;
    private final RowType rowType;
    private final long maxMemory;
    private final int pageSize;
    private final int spillSortMaxNumFiles;
    private final String spillCompression;
    private final MemorySize maxDiskSize;

    private transient BinaryExternalSortBuffer buffer;
    private transient IOManager ioManager;

    public SortBufferManager(
            RowType keyType,
            RowType rowType,
            long maxMemory,
            int pageSize,
            int spillSortMaxNumFiles,
            String spillCompression,
            MemorySize maxDiskSize) {
        this.keyType = keyType;
        this.rowType = rowType;
        this.maxMemory = maxMemory;
        this.pageSize = pageSize;
        this.spillSortMaxNumFiles = spillSortMaxNumFiles;
        this.spillCompression = spillCompression;
        this.maxDiskSize = maxDiskSize;
    }

    public void open(String[] spillPaths) {
        this.ioManager = IOManager.create(spillPaths);
        this.buffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        rowType,
                        IntStream.range(0, keyType.getFieldCount()).toArray(),
                        maxMemory,
                        pageSize,
                        spillSortMaxNumFiles,
                        spillCompression,
                        maxDiskSize);
    }

    public void write(InternalRow record) throws IOException {
        buffer.write(record);
    }

    public MutableObjectIterator<BinaryRow> sortedIterator() throws IOException {
        return buffer.sortedIterator();
    }

    public int size() {
        return buffer == null ? 0 : buffer.size();
    }

    public int getArity() {
        return rowType.getFieldCount();
    }

    public void close() throws Exception {
        if (buffer != null) {
            buffer.clear();
        }
        if (ioManager != null) {
            ioManager.close();
        }
    }
}

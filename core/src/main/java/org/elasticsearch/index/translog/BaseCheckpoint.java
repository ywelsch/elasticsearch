/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.Channels;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class BaseCheckpoint {

    final long offset;
    final int numOps;
    final long generation;
    final long minTranslogGeneration;

    protected static final int CURRENT_VERSION = 2; // introduction of minTranslogGeneration

    protected static final String CHECKPOINT_CODEC = "ckp";

    static final int FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + Long.BYTES // minimum translog generation in the translog - introduced in 6.0.0
        + CodecUtil.footerLength();

    /**
     * Create a new translog checkpoint.
     *
     * @param offset           the current offset in the translog
     * @param numOps           the current number of operations in the translog
     * @param generation       the current translog generation
     * @param minTranslogGeneration the minimum generation referenced by the translog at this moment.
     */
    public BaseCheckpoint(long offset, int numOps, long generation, long minTranslogGeneration) {
        assert minTranslogGeneration <= generation :
            "minTranslogGen [" + minTranslogGeneration + "] is higher than generation [" + generation + "]";
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
    }

    public BaseCheckpoint(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.numOps = in.readInt();
        this.generation = in.readLong();
        this.minTranslogGeneration = in.readLong();
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
        writeAdditionalFields(out);
        out.writeLong(minTranslogGeneration);
    }

    protected int fileSize() {
        return FILE_SIZE;
    }

    protected void writeAdditionalFields(DataOutput out) throws IOException {

    }

    public static <T extends BaseCheckpoint> T read(Path path, CheckedFunction<DataInput, T, IOException> checkpointSupplier)
        throws IOException {
        try (Directory dir = new SimpleFSDirectory(path.getParent())) {
            try (IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, CURRENT_VERSION, CURRENT_VERSION);
                assert fileVersion == CURRENT_VERSION : fileVersion;
                T checkpoint = checkpointSupplier.apply(indexInput);
                assert indexInput.length() == checkpoint.fileSize() : indexInput.length();
                return checkpoint;
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, BaseCheckpoint checkpoint, OpenOption... options) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(checkpoint.fileSize()) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (OutputStreamIndexOutput indexOutput =
                 new OutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream, checkpoint.fileSize())) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, CURRENT_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == checkpoint.fileSize() :
                "get you numbers straight; bytes written: " + indexOutput.getFilePointer() + ", buffer size: " + checkpoint.fileSize();
            assert indexOutput.getFilePointer() < 512 :
                "checkpoint files have to be smaller than 512 bytes for atomic writes; size: " + indexOutput.getFilePointer();

        }
        // now go and write to the channel, in one go.
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(byteOutputStream.toByteArray(), channel);
            // no need to force metadata, file size stays the same and we did the full fsync
            // when we first created the file, so the directory entry doesn't change as well
            channel.force(false);
        }
    }

    public static InitialCheckpointSupplier emptyTranslogCheckpoint() {
        return (offset, generation, minTranslogGeneration) -> new BaseCheckpoint(offset, 0, generation, minTranslogGeneration);
    }

    interface InitialCheckpointSupplier {
        BaseCheckpoint get(long offset, long generation, long minTranslogGeneration);
    }

}

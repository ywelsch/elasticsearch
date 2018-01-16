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
package org.elasticsearch.discovery.zen2;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.translog.ChannelFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

final class Checkpoint {

    final long generation;
    final long offset;
    final long term;

    private static final int INITIAL_VERSION = 1;

    private static final String CHECKPOINT_CODEC = "ckp";

    static final int FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Long.BYTES  // generation
        + Long.BYTES // offset
        + Long.BYTES // term
        + CodecUtil.footerLength();

    /**
     * Create a new checkpoint for {@link ConsensusStorage}.
     *
     * @param generation            the current translog generation
     * @param offset                the current offset in the translog
     * @param term                  the current term
     */
    Checkpoint(long generation, long offset, long term) {
        this.generation = generation;
        this.offset = offset;
        this.term = term;
    }

    Checkpoint(DataInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readLong());
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(generation);
        out.writeLong(offset);
        out.writeLong(term);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
            "offset=" + offset +
            ", generation=" + generation +
            ", term=" + term +
            '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (Directory dir = new SimpleFSDirectory(path.getParent())) {
            try (IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, INITIAL_VERSION, INITIAL_VERSION);
                return new Checkpoint(indexInput);
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(FILE_SIZE) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (OutputStreamIndexOutput indexOutput =
                 new OutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream, FILE_SIZE)) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, INITIAL_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == FILE_SIZE :
                "get you numbers straight; bytes written: " + indexOutput.getFilePointer() + ", buffer size: " + FILE_SIZE;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (generation != that.generation) return false;
        if (offset != that.offset) return false;
        return term == that.term;
    }

    @Override
    public int hashCode() {
        int result = (int) (generation ^ (generation >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }

}

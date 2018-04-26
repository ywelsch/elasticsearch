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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.zen2.ConsensusState.BasePersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TruncatedTranslogException;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ConsensusStorage extends AbstractComponent implements PersistedState, Closeable {

    public static final int VERSION = 1;
    public static final String LOG_CODEC = "cslog";
    public static final String LOG_SUFFIX = ".log";
    public static final String LOG_FILE_PREFIX = "cslog";
    public static final String CHECKPOINT_SUFFIX = ".ckp";
    public static final String CHECKPOINT_FILE_NAME = "cslog" + CHECKPOINT_SUFFIX;

    public static final Setting<ByteSizeValue> CS_LOG_RETENTION_SIZE_SETTING =
        Setting.byteSizeSetting("discovery.zen2.retention.size", new ByteSizeValue(128, ByteSizeUnit.MB), Setting.Property.NodeScope);

    private final Path path;
    private final DiscoveryNode localNode;
    private final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

    private PersistedState persistedState;
    private Writer writer;
    private boolean closed = false;

    public ConsensusStorage(Settings settings, Path path, DiscoveryNode localNode) {
        super(settings);
        this.path = path;
        this.localNode = localNode;
    }

    public Path getPath() {
        return path;
    }

    public boolean hasStore() {
        ensureOpen();
        return Files.exists(path.resolve(CHECKPOINT_FILE_NAME));
    }

    public void createFreshStore(long term, ClusterState clusterState) {
        ensureOpen();
        assert hasStore() == false;
        try {
            persistedState = new BasePersistedState(term, clusterState);
            createFreshGeneration(clusterState);
            writeCheckpoint(persistedState, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
    }

    public void recoverFromExistingStore(NamedWriteableRegistry registry) {
        ensureOpen();
        try {
            final Checkpoint checkpoint = Checkpoint.read(path.resolve(CHECKPOINT_FILE_NAME));
            persistedState = new Reader(path, checkpoint, registry).recover(getChannelFactory(), localNode);
            /* Clean up previous failures only after we've successfully recovered.
             *
             * There might be a dangling log file from the previous generation (n - 1) if we crashed just after
             * rolling a generation but before cleaning up the previous generation.
             * There might be a dangling log file from the next generation (n + 1) if we crashed after creating
             * the log file for the next generation but before writing the checkpoint file pointing to the new generation.
             *
             * All other dangling log files are unexpected and will lead to hard failures when
             * rolling over (as we use the CREATE_NEW flag for opening the new log file).
             *
             * TODO: check if there are other segment files in the directory and refuse to recover
             */
            Files.deleteIfExists(path.resolve(logFileName(checkpoint.generation - 1)));
            Files.deleteIfExists(path.resolve(logFileName(checkpoint.generation + 1)));
            writer = new Writer(path, getChannelFactory(), checkpoint.generation, checkpoint.offset);
        } catch (IOException e) {
            closeWithTragicEvent(e);
            throw new UncheckedIOException("failed to recover", e);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
    }

    private void writeCheckpoint(PersistedState persistedState, OpenOption... extraOptions) {
        try {
            Checkpoint.write(getChannelFactory(), path.resolve(CHECKPOINT_FILE_NAME),
                new Checkpoint(writer.generation, writer.getCurrentOffset(), persistedState.getCurrentTerm()),
                extraOptions);
        } catch (IOException e) {
            closeWithTragicEvent(e);
            throw new UncheckedIOException("failed to write checkpoint file", e);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
    }

    long getGeneration() {
        return writer.generation;
    }

    private void createFreshGeneration(ClusterState clusterState) {
        try {
            final long nextGeneration = writer == null ? 0L : writer.generation + 1;
            writer = new Writer(path, getChannelFactory(), nextGeneration, 0L);
            try (ReleasablePagedBytesReference bytes = serialize(new FullClusterState(clusterState))) {
                writer.add(bytes);
            }
            IOUtils.fsync(path, true);
        } catch (IOException e) {
            closeWithTragicEvent(e);
            throw new UncheckedIOException("failed to create fresh generation", e);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
    }

    static int getHeaderLength() {
        return CodecUtil.headerLength(LOG_CODEC);
    }

    static String logFileName(long generation) {
        return LOG_FILE_PREFIX + "-" + generation + LOG_SUFFIX;
    }

    @Override
    public void setCurrentTerm(long term) {
        ensureOpen();
        PersistedState newPersistedState = new BasePersistedState(persistedState);
        newPersistedState.setCurrentTerm(term);
        writeCheckpoint(newPersistedState, StandardOpenOption.WRITE);
        this.persistedState = newPersistedState;
    }

    @Override
    public void setLastAcceptedState(ClusterState acceptedState) {
        ensureOpen();
        PersistedState newPersistedState = new BasePersistedState(persistedState);
        newPersistedState.setLastAcceptedState(acceptedState);
        ClusterDiff diff = new ClusterDiff(acceptedState.diff(persistedState.getLastAcceptedState()));
        appendOrRollover(newPersistedState, diff);
        this.persistedState = newPersistedState;
    }

    @Override
    public void markLastAcceptedConfigAsCommitted() {
        ensureOpen();
        PersistedState newPersistedState = new BasePersistedState(persistedState);
        newPersistedState.markLastAcceptedConfigAsCommitted();
        appendOrRollover(newPersistedState, new Commit());
        this.persistedState = newPersistedState;
    }

    private void appendOrRollover(PersistedState persistedState, Entry entry) {
        boolean rollOver = false;
        try (ReleasablePagedBytesReference bytes = serialize(entry)) {
            ByteSizeValue maxSize = CS_LOG_RETENTION_SIZE_SETTING.get(settings);
            if (writer.getCurrentOffset() + bytes.length() > maxSize.getBytes()) {
                rollOver = true;
                if (bytes.length() > maxSize.getBytes()) {
                    logger.warn("log entry [{}] larger than cs log retention size [{}]", new ByteSizeValue(bytes.length()), maxSize);
                }
            } else {
                writer.add(bytes);
                writeCheckpoint(persistedState, StandardOpenOption.WRITE);
            }
        } catch (IOException e) {
            closeWithTragicEvent(e);
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
        if (rollOver) {
            rollOver(persistedState);
        }
    }

    private void rollOver(PersistedState persistedState) {
        try {
            writer.close();
            createFreshGeneration(persistedState.getLastAcceptedState());
            writeCheckpoint(persistedState, StandardOpenOption.WRITE);
            Files.delete(path.resolve(logFileName(writer.generation - 1)));
        } catch (IOException e) {
            closeWithTragicEvent(e);
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            closeWithTragicEvent(e);
            throw e;
        }
    }

    @Override
    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    @Override
    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }

    boolean isOpen() {
        return closed == false;
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("consensus storage already closed");
        }
    }

    private void closeWithTragicEvent(Exception e) {
        logger.warn("consensus storage encountered tragic event, closing...", e);
        close();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            IOUtils.closeWhileHandlingException(writer);
        }
    }

    // allows tests to inject failures
    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    private ReleasablePagedBytesReference serialize(Entry entry) {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            final long start = out.position();
            out.skip(Integer.BYTES);
            writeOperationNoSize(new BufferedChecksumStreamOutput(out), entry);
            final long end = out.position();
            final int operationSize = (int) (end - Integer.BYTES - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            return out.bytes();
        } catch (IOException ex) {
            closeWithTragicEvent(ex);
            throw new UncheckedIOException(ex);
        } catch (final Exception e) {
            closeWithTragicEvent(e);
            throw new IllegalStateException("Failed to serialize operation [" + entry + "]", e);
        }
    }

    private static void writeOperationNoSize(BufferedChecksumStreamOutput out, Entry entry) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        out.resetDigest();
        entry.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    public static class Writer implements Closeable {

        private final long generation;
        private final FileChannel channel;
        private final OutputStream outputStream;

        public Writer(Path path, ChannelFactory channelFactory, long generation, long initialOffset) throws IOException {
            this.generation = generation;
            final Path filePath = path.resolve(logFileName(generation));
            FileChannel channel = null;
            try {
                if (initialOffset > 0L) {
                    channel = channelFactory.open(filePath, StandardOpenOption.WRITE);
                    channel.position(initialOffset);
                    ByteSizeValue bufferSize = new ByteSizeValue(1, ByteSizeUnit.MB);
                    outputStream = new BufferedOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
                } else {
                    channel = channelFactory.open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                    // This OutputStreamDataOutput is intentionally not closed because closing it will close the FileChannel
                    final OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
                    CodecUtil.writeHeader(out, LOG_CODEC, VERSION);
                    channel.force(true);
                    ByteSizeValue bufferSize = new ByteSizeValue(1, ByteSizeUnit.MB);
                    outputStream = new BufferedOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
                    assert channel.position() == getHeaderLength();
                }
            } catch (Exception e) {
                IOUtils.close(channel);
                throw e;
            }
            this.channel = channel;
        }

        public long getCurrentOffset() throws IOException {
            return channel.position();
        }

        public void add(final BytesReference data) throws IOException {
            data.writeTo(outputStream);
            outputStream.flush();
            channel.force(false);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(channel);
        }
    }

    public static class Reader {

        private final Checkpoint checkpoint;
        private final Path path;
        private final NamedWriteableRegistry registry;

        private long currentOffset;
        private long firstOperationOffset;
        private FileChannel channel;

        public Reader(Path path, Checkpoint checkpoint, NamedWriteableRegistry registry) {
            this.path = path;
            this.checkpoint = checkpoint;
            this.registry = registry;
        }

        private PersistedState recover(ChannelFactory channelFactory, DiscoveryNode localNode) throws IOException {
            final AtomicReference<PersistedState> persistedState = new AtomicReference<>();
            currentOffset = checkpoint.offset;
            final Consumer<Entry> stateRecovery = entry -> {
                if (persistedState.get() == null) {
                    assert entry instanceof FullClusterState;
                    persistedState.set(
                        new BasePersistedState(checkpoint.term, ((FullClusterState) entry).clusterState));
                } else {
                    assert entry instanceof FullClusterState == false;
                    if (entry instanceof ClusterDiff) {
                        persistedState.get().setLastAcceptedState(((ClusterDiff) entry).clusterStateDiff
                            .apply(persistedState.get().getLastAcceptedState()));
                    } else if (entry instanceof Commit) {
                        persistedState.get().markLastAcceptedConfigAsCommitted();
                    } else {
                        assert false;
                        throw new IllegalStateException("Unexpected class " + entry.getClass().getName());
                    }
                }
            };
            channel = channelFactory.open(path.resolve(logFileName(checkpoint.generation)), StandardOpenOption.READ);
            try {
                checkHeader(channel, path.resolve(logFileName(checkpoint.generation)), checkpoint);
                firstOperationOffset = getHeaderLength();

                final ByteBuffer reusableBuffer = ByteBuffer.allocate(1024);
                BufferedChecksumStreamInput reuse = null;
                long position = firstOperationOffset;
                while (position < checkpoint.offset) {
                    final int opSize = readSize(reusableBuffer, position);
                    reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
                    stateRecovery.accept(readEntry(reuse, localNode, registry));
                    position += opSize;
                }
            } finally {
                IOUtils.close(channel);
                channel = null;
            }
            return persistedState.get();
        }

        public static void checkHeader(final FileChannel channel, final Path path, final Checkpoint checkpoint) throws IOException {
            try {
                InputStreamStreamInput headerStream = new InputStreamStreamInput(java.nio.channels.Channels.newInputStream(channel),
                    channel.size()); // don't close
                int version = CodecUtil.checkHeader(new InputStreamDataInput(headerStream), LOG_CODEC, 1, Integer.MAX_VALUE);
                if (version == VERSION) {
                    assert checkpoint.offset <= channel.size() :
                        "checkpoint is inconsistent with channel length: " + channel.size() + " " + checkpoint;
                } else {
                    throw new TranslogCorruptedException("No known cslog stream version: " + version + " path:" + path);
                }
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
                throw new TranslogCorruptedException("cslog header corrupted. path:" + path, e);
            }
        }

        /**
         * reads an operation at the given position and returns it. The buffer length is equal to the number
         * of bytes reads.
         */
        protected final BufferedChecksumStreamInput checksummedStream(ByteBuffer reusableBuffer, long position, int opSize,
                                                                      BufferedChecksumStreamInput reuse) throws IOException {
            final ByteBuffer buffer;
            if (reusableBuffer.capacity() >= opSize) {
                buffer = reusableBuffer;
            } else {
                buffer = ByteBuffer.allocate(opSize);
            }
            buffer.clear();
            buffer.limit(opSize);
            readBytes(buffer, position);
            buffer.flip();
            StreamInput in = new ByteBufferStreamInput(buffer);
            return new BufferedChecksumStreamInput(in, reuse);
        }

        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            if (position >= currentOffset) {
                throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + currentOffset + "], generation: [" +
                    checkpoint.generation + "], path: [" + path + "]");
            }
            if (position < firstOperationOffset) {
                throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" +
                    firstOperationOffset + "], generation: [" + checkpoint.generation + "], path: [" + path + "]");
            }
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        }

        /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
        protected final int readSize(ByteBuffer reusableBuffer, long position) throws IOException {
            // read op size from disk
            assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" +
                reusableBuffer.capacity() + "]";
            reusableBuffer.clear();
            reusableBuffer.limit(4);
            readBytes(reusableBuffer, position);
            reusableBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            final int size = reusableBuffer.getInt() + 4;
            final long maxSize = currentOffset - position;
            if (size < 0 || size > maxSize) {
                throw new TranslogCorruptedException("operation size is corrupted must be [0.." + maxSize + "] but was: " + size);
            }
            return size;
        }

        static Entry readEntry(BufferedChecksumStreamInput in, DiscoveryNode localNode,
                               NamedWriteableRegistry registry) throws IOException {
            final Entry entry;
            try {
                final int opSize = in.readInt();
                if (opSize < 4) { // 4byte for the checksum
                    throw new TranslogCorruptedException("operation size must be at least 4 but was: " + opSize);
                }
                in.resetDigest(); // size is not part of the checksum!

                in.mark(opSize);
                in.skip(opSize - 4);
                verifyChecksum(in);
                in.reset();

                entry = Entry.readEntry(new NamedWriteableAwareStreamInput(in, registry), localNode);
                verifyChecksum(in);
            } catch (TranslogCorruptedException e) {
                throw e;
            } catch (EOFException e) {
                throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
            }
            return entry;
        }

        private static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
            // This absolutely must come first, or else reading the checksum becomes part of the checksum
            long expectedChecksum = in.getChecksum();
            long readChecksum = in.readInt() & 0xFFFF_FFFFL;
            if (readChecksum != expectedChecksum) {
                throw new TranslogCorruptedException("cslog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
            }
        }
    }

    interface Entry extends Writeable {

        static Entry readEntry(StreamInput streamInput, DiscoveryNode localNode) throws IOException {
            byte type = streamInput.readByte();
            switch (type) {
                case ClusterDiff.TYPE_ID: return new ClusterDiff(streamInput, localNode);
                case Commit.TYPE_ID: return new Commit();
                case FullClusterState.TYPE_ID: return new FullClusterState(streamInput, localNode);
                default: throw new IllegalArgumentException();
            }
        }
    }


    static class ClusterDiff implements Entry {

        static final byte TYPE_ID = 0;

        private final Diff<ClusterState> clusterStateDiff;

        ClusterDiff(Diff<ClusterState> clusterStateDiff) {
            this.clusterStateDiff = clusterStateDiff;
        }

        ClusterDiff(StreamInput streamInput, DiscoveryNode localNode) throws IOException {
            this.clusterStateDiff = ClusterState.readDiffFrom(streamInput, localNode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TYPE_ID);
            clusterStateDiff.writeTo(out);
        }

    }

    static class Commit implements Entry {

        static final byte TYPE_ID = 1;

        Commit() {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TYPE_ID);
        }
    }

    static class FullClusterState implements Entry {

        static final byte TYPE_ID = 2;

        private final ClusterState clusterState;

        FullClusterState(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        FullClusterState(StreamInput streamInput, DiscoveryNode localNode) throws IOException {
            this.clusterState = ClusterState.readFrom(streamInput, localNode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(TYPE_ID);
            clusterState.writeTo(out);
        }
    }
}

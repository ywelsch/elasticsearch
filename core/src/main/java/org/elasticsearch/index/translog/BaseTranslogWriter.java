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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class BaseTranslogWriter extends BaseTranslogReader implements Closeable {

    public static final String TRANSLOG_CODEC = "translog";
    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;

    private final ChannelFactory channelFactory;
    // the last checkpoint that was written when the translog was last synced
    protected volatile BaseCheckpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    protected volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private volatile Exception tragedy;
    /* A buffered outputstream what writes to the writers channel */
    private final OutputStream outputStream;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    protected volatile long totalOffset;

    protected final LongSupplier minTranslogGenerationSupplier;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order synchronized(syncLock) -> synchronized(this)
    private final Object syncLock = new Object();

    protected BaseTranslogWriter(
        final ChannelFactory channelFactory,
        final BaseCheckpoint initialCheckpoint,
        final FileChannel channel,
        final Path path,
        final ByteSizeValue bufferSize,
        final LongSupplier minTranslogGenerationSupplier) throws IOException {
        super(initialCheckpoint.generation, channel, path, channel.position());
        assert initialCheckpoint.offset == channel.position() :
            "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel poistion ["
                + channel.position() + "]";
        this.channelFactory = channelFactory;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.outputStream = new BufferedChannelOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
    }

    static int getHeaderLength(String translogUUID) {
        return getHeaderLength(new BytesRef(translogUUID).length);
    }

    static int getHeaderLength(int uuidLength) {
        return CodecUtil.headerLength(TRANSLOG_CODEC) + uuidLength + Integer.BYTES;
    }

    static void writeHeader(OutputStreamDataOutput out, BytesRef ref) throws IOException {
        CodecUtil.writeHeader(out, TRANSLOG_CODEC, VERSION);
        out.writeInt(ref.length);
        out.writeBytes(ref.bytes, ref.offset, ref.length);
    }

    public static BaseTranslogWriter create(String translogUUID, long fileGeneration, Path file, ChannelFactory channelFactory,
                                            ByteSizeValue bufferSize, BaseCheckpoint.InitialCheckpointSupplier initialCheckpointSupplier,
                                            final long initialMinTranslogGen, final LongSupplier minTranslogGenerationSupplier)
        throws IOException {
        final BytesRef ref = new BytesRef(translogUUID);
        final int firstOperationOffset = getHeaderLength(ref.length);
        final FileChannel channel = channelFactory.open(file);
        try {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            final OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            writeHeader(out, ref);
            channel.force(true);
            final BaseCheckpoint checkpoint = initialCheckpointSupplier.get(firstOperationOffset, fileGeneration, initialMinTranslogGen);
            writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            return new BaseTranslogWriter(channelFactory, checkpoint, channel, file, bufferSize, minTranslogGenerationSupplier);
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation is an error condition
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    /**
     * If this {@code BaseTranslogWriter} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Exception getTragicException() {
        return tragedy;
    }

    private synchronized void closeWithTragicEvent(Exception exception) throws IOException {
        assert exception != null;
        if (tragedy == null) {
            tragedy = exception;
        } else if (tragedy != exception) {
            // it should be safe to call closeWithTragicEvents on multiple layers without
            // worrying about self suppression.
            tragedy.addSuppressed(exception);
        }
        close();
    }

    /**
     * add the given bytes to the translog and return the location they were written at
     */
    public synchronized BaseTranslog.Location add(final BytesReference data) throws IOException {
        ensureOpen();
        final long offset = totalOffset;
        try {
            data.writeTo(outputStream);
        } catch (final Exception ex) {
            try {
                closeWithTragicEvent(ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        }
        totalOffset += data.length();
        operationCounter++;

        return new BaseTranslog.Location(generation, offset, data.length());
    }

    /**
     * write all buffered ops to disk and fsync file.
     *
     * Note: any exception during the sync process will be interpreted as a tragic exception and the writer will be closed before
     * raising the exception.
     */
    public void sync() throws IOException {
        syncUpTo(Long.MAX_VALUE);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     */
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset ||
            minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized BaseCheckpoint getCheckpoint() {
        return new BaseCheckpoint(totalOffset, operationCounter, generation, minTranslogGenerationSupplier.getAsLong());
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    /**
     * Closes this writer and transfers its underlying file channel to a new immutable {@link TranslogReader}
     * @return a new {@link TranslogReader}
     * @throws IOException if any of the file operations resulted in an I/O exception
     */
    public TranslogReader closeIntoReader() throws IOException {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        //       we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            synchronized (this) {
                try {
                    sync(); // sync before we close..
                } catch (IOException e) {
                    try {
                        closeWithTragicEvent(e);
                    } catch (Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
                if (closed.compareAndSet(false, true)) {
                    return new TranslogReader(getLastSyncedCheckpoint(), channel, path, getFirstOperationOffset());
                } else {
                    throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed (path [" + path + "]", tragedy);
                }
            }
        }
    }


    @Override
    public TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        synchronized (syncLock) {
            synchronized (this) {
                ensureOpen();
                try {
                    sync();
                } catch (IOException e) {

                    throw createTranslogException("exception while syncing before creating a snapshot", e);
                }
                return super.newSnapshot();
            }
        }
    }

    protected TranslogException createTranslogException(String reason, Exception cause) {
        return new TranslogException(reason, cause);
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final BaseCheckpoint checkpointToSync;
                    synchronized (this) {
                        ensureOpen();
                        try {
                            outputStream.flush();
                            checkpointToSync = getCheckpoint();
                        } catch (Exception ex) {
                            try {
                                closeWithTragicEvent(ex);
                            } catch (Exception inner) {
                                ex.addSuppressed(inner);
                            }
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        channel.force(false);
                        writeCheckpoint(channelFactory, path.getParent(), checkpointToSync);
                    } catch (Exception ex) {
                        try {
                            closeWithTragicEvent(ex);
                        } catch (Exception inner) {
                            ex.addSuppressed(inner);
                        }
                        throw ex;
                    }
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset :
                        "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        if (position + targetBuffer.remaining() > getWrittenOffset()) {
            synchronized (this) {
                // we only flush here if it's really really needed - try to minimize the impact of the read operation
                // in some cases ie. a tragic event we might still be able to read the relevant value
                // which is not really important in production but some test can make most strict assumptions
                // if we don't fail in this call unless absolutely necessary.
                if (position + targetBuffer.remaining() > getWrittenOffset()) {
                    outputStream.flush();
                }
            }
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    protected static void writeCheckpoint(
            final ChannelFactory channelFactory,
            final Path translogFile,
            final BaseCheckpoint checkpoint) throws IOException {
        BaseCheckpoint.write(channelFactory, translogFile.resolve(BaseTranslog.CHECKPOINT_FILE_NAME), checkpoint, StandardOpenOption.WRITE);
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    BaseCheckpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy);
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            channel.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }


    private final class BufferedChannelOutputStream extends BufferedOutputStream {

        BufferedChannelOutputStream(OutputStream out, int size) throws IOException {
            super(out, size);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (count > 0) {
                try {
                    ensureOpen();
                    super.flush();
                } catch (Exception ex) {
                    try {
                        closeWithTragicEvent(ex);
                    } catch (Exception inner) {
                        ex.addSuppressed(inner);
                    }
                    throw ex;
                }
            }
        }

        @Override
        public void close() throws IOException {
            // the stream is intentionally not closed because
            // closing it will close the FileChannel
            throw new IllegalStateException("never close this stream");
        }
    }

}

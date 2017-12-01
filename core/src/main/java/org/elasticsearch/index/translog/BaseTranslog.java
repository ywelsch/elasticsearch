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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A Translog is a component that records a series of operations in a durable manner.
 * In Elasticsearch there is one Translog instance per {@link org.elasticsearch.index.engine.InternalEngine}. The engine
 * records the current translog generation {@link BaseTranslog#getGeneration()} in it's commit metadata using {@link #TRANSLOG_GENERATION_KEY}
 * to reference the generation that contains all operations that have not yet successfully been committed to the engines lucene index.
 * Additionally, since Elasticsearch 2.0 the engine also records a {@link #TRANSLOG_UUID_KEY} with each commit to ensure a strong association
 * between the lucene index an the transaction log file. This UUID is used to prevent accidental recovery from a transaction log that belongs to a
 * different engine.
 * <p>
 * Each Translog has only one translog file open for writes at any time referenced by a translog generation ID. This ID is written to a
 * <tt>translog.ckp</tt> file that is designed to fit in a single disk block such that a write of the file is atomic. The checkpoint file
 * is written on each fsync operation of the translog and records the number of operations written, the current translog's file generation,
 * its fsynced offset in bytes, and other important statistics.
 * </p>
 * <p>
 * When the current translog file reaches a certain size, the current file is reopened for read only and a new
 * write only file is created. Any non-current, read only translog file always has a <tt>translog-${gen}.ckp</tt> associated with it
 * which is an fsynced copy of its last <tt>translog.ckp</tt> such that in disaster recovery last fsynced offsets, number of
 * operation etc. are still preserved.
 * </p>
 */
public class BaseTranslog extends AbstractComponent implements Closeable {

    /*
     * TODO
     *  - we might need something like a deletion policy to hold on to more than one translog eventually (I think sequence IDs needs this) but we can refactor as we go
     *  - use a simple BufferedOutputStream to write stuff and fold BufferedTranslogWriter into it's super class... the tricky bit is we need to be able to do random access reads even from the buffer
     *  - we need random exception on the FileSystem API tests for all this.
     *  - we need to page align the last write before we sync, we can take advantage of ensureSynced for this since we might have already fsynced far enough
     */
    public static final String TRANSLOG_GENERATION_KEY = "translog_generation";
    public static final String TRANSLOG_UUID_KEY = "translog_uuid";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";
    public static final String CHECKPOINT_SUFFIX = ".ckp";
    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");

    // the list of translog readers is guaranteed to be in order of translog generation
    protected final List<TranslogReader> readers = new ArrayList<>();
    private BigArrays bigArrays;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    private final Path location;
    protected BaseTranslogWriter current;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BaseTranslogConfig config;
    protected final String translogUUID;
    private final TranslogDeletionPolicy deletionPolicy;
    private final BaseCheckpoint.InitialCheckpointSupplier initialCheckpointSupplier;
    private final CheckedFunction<DataInput, ? extends BaseCheckpoint, IOException> checkpointReader;

    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                   the configuration of this translog
     * @param expectedTranslogUUID     the translog uuid to open, null for a new translog
     * @param deletionPolicy           an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                 deleted
     */
    public BaseTranslog(
        final BaseTranslogConfig config, final String expectedTranslogUUID, TranslogDeletionPolicy deletionPolicy,
        final BaseCheckpoint.InitialCheckpointSupplier initialCheckpointSupplier,
        final CheckedFunction<DataInput, ? extends BaseCheckpoint, IOException> checkpointReader) throws IOException {
        super(config.getSettings());
        this.config = config;
        this.initialCheckpointSupplier = initialCheckpointSupplier;
        this.checkpointReader = checkpointReader;
        this.deletionPolicy = deletionPolicy;
        if (expectedTranslogUUID == null) {
            translogUUID = UUIDs.randomBase64UUID();
        } else {
            translogUUID = expectedTranslogUUID;
        }
        bigArrays = config.getBigArrays();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();
        Files.createDirectories(this.location);

        try {
            if (expectedTranslogUUID != null) {
                final BaseCheckpoint checkpoint = BaseCheckpoint.read(location.resolve(CHECKPOINT_FILE_NAME), checkpointReader);
                final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
                final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
                // this is special handling for error condition when we create a new writer but we fail to bake
                // the newly written file (generation+1) into the checkpoint. This is still a valid state
                // we just need to cleanup before we continue
                // we hit this before and then blindly deleted the new generation even though we managed to bake it in and then hit this:
                // https://discuss.elastic.co/t/cannot-recover-index-because-of-missing-tanslog-files/38336 as an example
                //
                // For this to happen we must have already copied the translog.ckp file into translog-gen.ckp so we first check if that file exists
                // if not we don't even try to clean it up and wait until we fail creating it
                assert Files.exists(nextTranslogFile) == false || Files.size(nextTranslogFile) <= BaseTranslogWriter.getHeaderLength(expectedTranslogUUID) : "unexpected translog file: [" + nextTranslogFile + "]";
                if (Files.exists(currentCheckpointFile) // current checkpoint is already copied
                        && Files.deleteIfExists(nextTranslogFile)) { // delete it and log a warning
                    logger.warn("deleted previously created, but not yet committed, next generation [{}]. This can happen due to a tragic exception when creating a new generation", nextTranslogFile.getFileName());
                }
                this.readers.addAll(recoverFromFiles(checkpoint));
                if (readers.isEmpty()) {
                    throw new IllegalStateException("at least one reader must be recovered");
                }
                boolean success = false;
                current = null;
                try {
                    current = createWriter(checkpoint.generation + 1);
                    success = true;
                } finally {
                    // we have to close all the recovered ones otherwise we leak file handles here
                    // for instance if we have a lot of tlog and we can't create the writer we keep on holding
                    // on to all the uncommitted tlog files if we don't close
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(readers);
                    }
                }
            } else {
                IOUtils.rm(location);
                // start from whatever generation lucene points to
                final long generation = deletionPolicy.getMinTranslogGenerationForRecovery();
                logger.debug("wipe translog location - creating new translog, starting generation [{}]", generation);
                Files.createDirectories(location);
                final BaseCheckpoint checkpoint = initialCheckpointSupplier.get(0, generation, generation);
                final Path checkpointFile = location.resolve(CHECKPOINT_FILE_NAME);
                BaseCheckpoint.write(getChannelFactory(), checkpointFile, checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                IOUtils.fsync(checkpointFile, false);
                current = createWriter(generation, generation);
                readers.clear();
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    /** recover all translog files found on disk */
    private ArrayList<TranslogReader> recoverFromFiles(BaseCheckpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, TRANSLOG_FILE_SUFFIX); // a temp file to copy checkpoint to - note it must be in on the same FS otherwise atomic move won't work
        boolean tempFileRenamed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);

            final long minGenerationToRecoverFrom;
            if (checkpoint.minTranslogGeneration < 0) {
                minGenerationToRecoverFrom = deletionPolicy.getMinTranslogGenerationForRecovery();
            } else {
                minGenerationToRecoverFrom = checkpoint.minTranslogGeneration;
            }

            final String checkpointTranslogFile = getFilename(checkpoint.generation);
            // we open files in reverse order in order to validate tranlsog uuid before we start traversing the translog based on
            // the generation id we found in the lucene commit. This gives for better error messages if the wrong
            // translog was found.
            foundTranslogs.add(openReader(location.resolve(checkpointTranslogFile), checkpoint));
            for (long i = checkpoint.generation - 1; i >= minGenerationToRecoverFrom; i--) {
                Path committedTranslogFile = location.resolve(getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new IllegalStateException("translog file doesn't exist with generation: " + i + " recovering from: " +
                        minGenerationToRecoverFrom + " checkpoint: " + checkpoint.generation + " - translog ids must be consecutive");
                }
                final TranslogReader reader = openReader(committedTranslogFile, BaseCheckpoint.read(location.resolve(getCommitCheckpointFileName(i)), checkpointReader));
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            Collections.reverse(foundTranslogs);

            // when we clean up files, we first update the checkpoint with a new minReferencedTranslog and then delete them;
            // if we crash just at the wrong moment, it may be that we leave one unreferenced file behind so we delete it if there
            IOUtils.deleteFilesIgnoringExceptions(location.resolve(getFilename(minGenerationToRecoverFrom - 1)),
                location.resolve(getCommitCheckpointFileName(minGenerationToRecoverFrom - 1)));

            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                BaseCheckpoint checkpointFromDisk = BaseCheckpoint.read(commitCheckpoint, checkpointReader);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new IllegalStateException("Checkpoint file " + commitCheckpoint.getFileName() + " already exists but has corrupted content expected: " + checkpoint + " but got: " + checkpointFromDisk);
                }
            } else {
                // we first copy this into the temp-file and then fsync it followed by an atomic move into the target file
                // that way if we hit a disk-full here we are still in an consistent state.
                Files.copy(location.resolve(CHECKPOINT_FILE_NAME), tempFile, StandardCopyOption.REPLACE_EXISTING);
                IOUtils.fsync(tempFile, false);
                Files.move(tempFile, commitCheckpoint, StandardCopyOption.ATOMIC_MOVE);
                tempFileRenamed = true;
                // we only fsync the directory the tempFile was already fsynced
                IOUtils.fsync(commitCheckpoint.getParent(), true);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to delete temp file {}", tempFile), ex);
                }
            }
        }
        return foundTranslogs;
    }

    TranslogReader openReader(Path path, BaseCheckpoint checkpoint) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            assert BaseTranslog.parseIdFromFileName(path) == checkpoint.generation : "expected generation: " + BaseTranslog.parseIdFromFileName(path) + " but got: " + checkpoint.generation;
            TranslogReader reader = TranslogReader.open(channel, path, checkpoint, translogUUID);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }

    /**
     * Extracts the translog generation from a file name.
     *
     * @throws IllegalArgumentException if the path doesn't match the expected pattern.
     */
    public static long parseIdFromFileName(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_STRICT_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " + fileName + "]", e);
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }

    /** Returns {@code true} if this {@code BaseTranslog} is still open. */
    public boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    current.sync();
                } finally {
                    closeFilesIfNoPendingRetentionLocks();
                }
            } finally {
                logger.debug("translog closed");
            }
        }
    }

    /**
     * Returns all translog locations as absolute paths.
     * These paths don't contain actual translog files they are
     * directories holding the transaction logs.
     */
    public Path location() {
        return location;
    }

    /**
     * Returns the generation of the current transaction log.
     */
    public long currentFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    /**
     * Returns the minimum file generation referenced by the translog
     */
    long getMinFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            if (readers.isEmpty()) {
                return current.getGeneration();
            } else {
                assert readers.stream().map(TranslogReader::getGeneration).min(Long::compareTo).get()
                    .equals(readers.get(0).getGeneration()) : "the first translog isn't the one with the minimum generation:" + readers;
                return readers.get(0).getGeneration();
            }
        }
    }


    /**
     * Returns the number of operations in the translog files that aren't committed to lucene.
     */
    public int uncommittedOperations() {
        return totalOperations(deletionPolicy.getMinTranslogGenerationForRecovery());
    }

    /**
     * Returns the size in bytes of the translog files that aren't committed to lucene.
     */
    public long uncommittedSizeInBytes() {
        return sizeInBytesByMinGen(deletionPolicy.getMinTranslogGenerationForRecovery());
    }

    /**
     * Returns the number of operations in the translog files
     */
    public int totalOperations() {
        return totalOperations(-1);
    }

    /**
     * Returns the size in bytes of the v files
     */
    public long sizeInBytes() {
        return sizeInBytesByMinGen(-1);
    }

    /**
     * Returns the number of operations in the transaction files that aren't committed to lucene..
     */
    private int totalOperations(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current))
                    .filter(r -> r.getGeneration() >= minGeneration)
                    .mapToInt(BaseTranslogReader::totalOperations)
                    .sum();
        }
    }

    /**
     * Returns the size in bytes of the translog files above the given generation
     */
    private long sizeInBytesByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current))
                    .filter(r -> r.getGeneration() >= minGeneration)
                    .mapToLong(BaseTranslogReader::sizeInBytes)
                    .sum();
        }
    }

    /**
     * Creates a new translog for the specified generation.
     *
     * @param fileGeneration the translog generation
     * @return a writer for the new translog
     * @throws IOException if creating the translog failed
     */
    BaseTranslogWriter createWriter(long fileGeneration) throws IOException {
        return createWriter(fileGeneration, getMinFileGeneration());
    }

    /**
     * creates a new writer
     *
     * @param fileGeneration        the generation of the write to be written
     * @param initialMinTranslogGen the minimum translog generation to be written in the first checkpoint. This is
     *                              needed to solve and initialization problem while constructing an empty translog.
     *                              With no readers and no current, a call to  {@link #getMinFileGeneration()} would not work.
     */
    private BaseTranslogWriter createWriter(long fileGeneration, long initialMinTranslogGen) throws IOException {
        try {
            return createWriter(
                translogUUID,
                fileGeneration,
                location.resolve(getFilename(fileGeneration)),
                getChannelFactory(),
                config.getBufferSize(),
                initialMinTranslogGen,
                this::getMinFileGeneration);
        } catch (final IOException e) {
            throw createTranslogException("failed to create new translog file", e);
        }
    }

    protected TranslogException createTranslogException(String reason, Exception cause) {
        return new TranslogException(reason, cause);
    }

    protected BaseTranslogWriter createWriter(String translogUUID, long fileGeneration, Path file,
                                              ChannelFactory channelFactory, ByteSizeValue bufferSize,
                                              long initialMinTranslogGen, LongSupplier minTranslogGenerationSupplier)
        throws IOException {
        final BytesRef ref = new BytesRef(translogUUID);
        final int firstOperationOffset = BaseTranslogWriter.getHeaderLength(ref.length);
        final BaseCheckpoint checkpoint = initialCheckpointSupplier.get(firstOperationOffset, fileGeneration, initialMinTranslogGen);
        final FileChannel channel = channelFactory.open(file);
        try {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            final OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            BaseTranslogWriter.writeHeader(out, ref);
            channel.force(true);
            BaseTranslogWriter.writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            return createWriter(channelFactory, checkpoint, channel, file, bufferSize, minTranslogGenerationSupplier);
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation is an error condition
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    protected BaseTranslogWriter createWriter(ChannelFactory channelFactory, BaseCheckpoint checkpoint, FileChannel channel,
                                              Path path, ByteSizeValue bufferSize, LongSupplier minTranslogGenerationSupplier) throws IOException {
        return new BaseTranslogWriter(channelFactory, checkpoint, channel, path, bufferSize, minTranslogGenerationSupplier);
    }

    /**
     * Adds an operation to the transaction log.
     *
     * @param operation the operation to add
     * @return the location of the operation in the translog
     * @throws IOException if adding the operation to the translog resulted in an I/O exception
     */
    public Location add(Writeable operation) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            final long start = out.position();
            out.skip(Integer.BYTES);
            writeOperationNoSize(new BufferedChecksumStreamOutput(out), operation);
            final long end = out.position();
            final int operationSize = (int) (end - Integer.BYTES - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            final ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock ignored = readLock.acquire()) {
                ensureOpen();
                return addToWriter(current, operation, bytes);
            }
        } catch (final AlreadyClosedException | IOException ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final Exception e) {
            try {
                closeOnTragicEvent(e);
            } catch (final Exception inner) {
                e.addSuppressed(inner);
            }
            throw createTranslogException("Failed to write operation [" + operation + "]", e);
        } finally {
            Releasables.close(out);
        }
    }

    protected Location addToWriter(BaseTranslogWriter current, Writeable operation, BytesReference bytes) throws IOException {
        return current.add(bytes);
    }

    /**
     * Writes all operations in the given iterable to the given output stream including the size of the array
     * use {@link #readOperations} to read it back.
     */
    public static void writeOperations(StreamOutput outStream, List<? extends Writeable> toWrite) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(BigArrays.NON_RECYCLING_INSTANCE);
        try {
            outStream.writeInt(toWrite.size());
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            for (Writeable op : toWrite) {
                out.reset();
                final long start = out.position();
                out.skip(Integer.BYTES);
                writeOperationNoSize(checksumStreamOutput, op);
                long end = out.position();
                int operationSize = (int) (out.position() - Integer.BYTES - start);
                out.seek(start);
                out.writeInt(operationSize);
                out.seek(end);
                ReleasablePagedBytesReference bytes = out.bytes();
                bytes.writeTo(outStream);
            }
        } finally {
            Releasables.close(out);
        }

    }

    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Writeable op) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        out.resetDigest();
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    /**
     * Reads a list of operations written with {@link #writeOperations(StreamOutput, List)}
     */
    public static <T> List<T> readOperations(StreamInput input, CheckedFunction<StreamInput, T, IOException> reader) throws IOException {
        ArrayList<T> operations = new ArrayList<>();
        int numOps = input.readInt();
        final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(input);
        for (int i = 0; i < numOps; i++) {
            operations.add(readOperation(checksumStreamInput, reader));
        }
        return operations;
    }

    static <T> T readOperation(BufferedChecksumStreamInput in, CheckedFunction<StreamInput, T, IOException> reader) throws IOException {
        final T operation;
        try {
            final int opSize = in.readInt();
            if (opSize < 4) { // 4byte for the checksum
                throw new TranslogCorruptedException("operation size must be at least 4 but was: " + opSize);
            }
            in.resetDigest(); // size is not part of the checksum!
            if (in.markSupported()) { // if we can we validate the checksum first
                // we are sometimes called when mark is not supported this is the case when
                // we are sending translogs across the network with LZ4 compression enabled - currently there is no way s
                // to prevent this unfortunately.
                in.mark(opSize);

                in.skip(opSize - 4);
                verifyChecksum(in);
                in.reset();
            }
            operation = reader.apply(in);
            verifyChecksum(in);
        } catch (TranslogCorruptedException e) {
            throw e;
        } catch (EOFException e) {
            throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
        }
        return operation;
    }

    /**
     * The a {@linkplain Location} that will sort after the {@linkplain Location} returned by the last write but before any locations which
     * can be returned by the next write.
     */
    public Location getLastWriteLocation() {
        try (ReleasableLock lock = readLock.acquire()) {
            /*
             * We use position = current - 1 and size = Integer.MAX_VALUE here instead of position current and size = 0 for two reasons:
             * 1. BaseTranslog.Location's compareTo doesn't actually pay attention to size even though it's equals method does.
             * 2. It feels more right to return a *position* that is before the next write's position rather than rely on the size.
             */
            return new Location(current.generation, current.sizeInBytes() - 1, Integer.MAX_VALUE);
        }
    }

    /**
     * Acquires a lock on the translog files, preventing them from being trimmed
     */
    public Closeable acquireRetentionLock() {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final long viewGen = getMinFileGeneration();
            return acquireTranslogGenFromDeletionPolicy(viewGen);
        }
    }

    protected Closeable acquireTranslogGenFromDeletionPolicy(long viewGen) {
        Releasable toClose = deletionPolicy.acquireTranslogGen(viewGen);
        return () -> {
            try {
                toClose.close();
            } finally {
                trimUnreferencedReaders();
                closeFilesIfNoPendingRetentionLocks();
            }
        };
    }

    /**
     * Sync's the translog.
     */
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        } catch (Exception ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        }
    }

    /**
     *  Returns <code>true</code> if an fsync is required to ensure durability of the translogs operations or it's metadata.
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    /** package private for testing */
    public static String getFilename(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + TRANSLOG_FILE_SUFFIX;
    }

    static String getCommitCheckpointFileName(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + CHECKPOINT_SUFFIX;
    }


    /**
     * Ensures that the given location has be synced / written to the underlying storage.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.generation == current.getGeneration()) { // if we have a new one it's already synced
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size);
            }
        } catch (Exception ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        }
        return false;
    }

    /**
     * Ensures that all locations in the given stream have been synced / written to the underlying storage.
     * This method allows for internal optimization to minimize the amount of fsync operations if multiple
     * locations must be synced.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Stream<Location> locations) throws IOException {
        final Optional<Location> max = locations.max(Location::compareTo);
        // we only need to sync the max location since it will sync all other
        // locations implicitly
        if (max.isPresent()) {
            return ensureSynced(max.get());
        } else {
            return false;
        }
    }

    private void closeOnTragicEvent(Exception ex) {
        if (current.getTragicException() != null) {
            try {
                close();
            } catch (AlreadyClosedException inner) {
                // don't do anything in this case. The AlreadyClosedException comes from BaseTranslogWriter and we should not add it as suppressed because
                // will contain the Exception ex as cause. See also https://github.com/elastic/elasticsearch/issues/15941
            } catch (Exception inner) {
                assert (ex != inner.getCause());
                ex.addSuppressed(inner);
            }
        }
    }

    /**
     * return stats
     */
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            return new TranslogStats(totalOperations(), sizeInBytes(), uncommittedOperations(), uncommittedSizeInBytes());
        }
    }

    public BaseTranslogConfig getConfig() {
        return config;
    }

    // public for testing
    public TranslogDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }


    public static class Location implements Comparable<Location> {

        public final long generation;
        public final long translogLocation;
        public final int size;

        public Location(long generation, long translogLocation, int size) {
            this.generation = generation;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        public String toString() {
            return "[generation: " + generation + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (generation == o.generation) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(generation, o.generation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Location location = (Location) o;

            if (generation != location.generation) {
                return false;
            }
            if (translogLocation != location.translogLocation) {
                return false;
            }
            return size == location.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(generation);
            result = 31 * result + Long.hashCode(translogLocation);
            result = 31 * result + size;
            return result;
        }
    }

    public enum Durability {

        /**
         * Async durability - translogs are synced based on a time interval.
         */
        ASYNC,
        /**
         * Request durability - translogs are synced for each high level request (bulk, index, delete)
         */
        REQUEST

    }

    protected static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    /**
     * Roll the current translog generation into a new generation. This does not commit the
     * translog.
     *
     * @throws IOException if an I/O exception occurred during any file operations
     */
    public void rollGeneration() throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            try {
                final TranslogReader reader = current.closeIntoReader();
                readers.add(reader);
                final Path checkpoint = location.resolve(CHECKPOINT_FILE_NAME);
                assert BaseCheckpoint.read(checkpoint, checkpointReader).generation == current.getGeneration();
                final Path generationCheckpoint =
                        location.resolve(getCommitCheckpointFileName(current.getGeneration()));
                Files.copy(checkpoint, generationCheckpoint);
                IOUtils.fsync(generationCheckpoint, false);
                IOUtils.fsync(generationCheckpoint.getParent(), true);
                // create a new translog file; this will sync it and update the checkpoint data;
                current = createWriter(current.getGeneration() + 1);
                logger.trace("current translog set to [{}]", current.getGeneration());
            } catch (final Exception e) {
                IOUtils.closeWhileHandlingException(this); // tragic event
                throw e;
            }
        }
    }

    /**
     * Trims unreferenced translog generations by asking {@link TranslogDeletionPolicy} for the minimum
     * required generation
     */
    public void trimUnreferencedReaders() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get()) {
                // we're shutdown potentially on some tragic event, don't delete anything
                return;
            }
            long minReferencedGen = deletionPolicy.minTranslogGenRequired(readers, current);
            assert minReferencedGen >= getMinFileGeneration() :
                "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] but the lowest gen available is ["
                    + getMinFileGeneration() + "]";
            assert minReferencedGen <= currentFileGeneration() :
                "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] which is higher than the current generation ["
                    + currentFileGeneration() + "]";


            for (Iterator<TranslogReader> iterator = readers.iterator(); iterator.hasNext(); ) {
                TranslogReader reader = iterator.next();
                if (reader.getGeneration() >= minReferencedGen) {
                    break;
                }
                iterator.remove();
                IOUtils.closeWhileHandlingException(reader);
                final Path translogPath = reader.path();
                logger.trace("delete translog file [{}], not referenced and not current anymore", translogPath);
                // The checkpoint is used when opening the translog to know which files should be recovered from.
                // We now update the checkpoint to ignore the file we are going to remove.
                // Note that there is a provision in recoverFromFiles to allow for the case where we synced the checkpoint
                // but crashed before we could delete the file.
                current.sync();
                deleteReaderFiles(reader);
            }
            assert readers.isEmpty() == false || current.generation == minReferencedGen :
                "all readers were cleaned but the minReferenceGen [" + minReferencedGen + "] is not the current writer's gen [" +
                    current.generation + "]";
        } catch (Exception ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        }
    }

    /**
     * deletes all files associated with a reader. package-private to be able to simulate node failures at this point
     */
    void deleteReaderFiles(TranslogReader reader) {
        IOUtils.deleteFilesIgnoringExceptions(reader.path(),
            reader.path().resolveSibling(getCommitCheckpointFileName(reader.getGeneration())));
    }

    void closeFilesIfNoPendingRetentionLocks() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get() && deletionPolicy.pendingTranslogRefCount() == 0) {
                logger.trace("closing files. translog is closed and there are no pending retention locks");
                ArrayList<Closeable> toClose = new ArrayList<>(readers);
                toClose.add(current);
                IOUtils.close(toClose);
            }
        }
    }

    /**
     * References a transaction log generation
     */
    public static final class TranslogGeneration {
        public final String translogUUID;
        public final long translogFileGeneration;

        public TranslogGeneration(String translogUUID, long translogFileGeneration) {
            this.translogUUID = translogUUID;
            this.translogFileGeneration = translogFileGeneration;
        }

    }

    /**
     * Returns the current generation of this translog. This corresponds to the latest uncommitted translog generation
     */
    public TranslogGeneration getGeneration() {
        try (ReleasableLock lock = writeLock.acquire()) {
            return new TranslogGeneration(translogUUID, currentFileGeneration());
        }
    }

    /**
     * Returns <code>true</code> iff the given generation is the current generation of this translog
     */
    public boolean isCurrent(TranslogGeneration generation) {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (generation != null) {
                if (generation.translogUUID.equals(translogUUID) == false) {
                    throw new IllegalArgumentException("commit belongs to a different translog: " + generation.translogUUID + " vs. " + translogUUID);
                }
                return generation.translogFileGeneration == currentFileGeneration();
            }
        }
        return false;
    }

    long getFirstOperationPosition() { // for testing
        return current.getFirstOperationOffset();
    }

    protected void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog is already closed", current.getTragicException());
        }
    }

    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    /**
     * If this {@code BaseTranslog} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Exception getTragicException() {
        return current.getTragicException();
    }

    /**
     * Returns the translog uuid used to associate a lucene index with a translog.
     */
    public String getTranslogUUID() {
        return translogUUID;
    }


    BaseTranslogWriter getCurrent() {
        return current;
    }

    List<TranslogReader> getReaders() {
        return readers;
    }
}

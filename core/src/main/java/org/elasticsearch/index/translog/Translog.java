package org.elasticsearch.index.translog;

import org.apache.lucene.index.Term;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public class Translog extends BaseTranslog implements IndexShardComponent {

    private final LongSupplier globalCheckpointSupplier;

    /**
     * Creates a new translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                    the configuration of this translog
     * @param expectedTranslogUUID      the translog uuid to open, null for a new translog
     * @param deletionPolicy            an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                  deleted
     * @param globalCheckpointSupplier  a supplier for the global checkpoint
     */
    public Translog(TranslogConfig config, String expectedTranslogUUID, TranslogDeletionPolicy deletionPolicy,
                    LongSupplier globalCheckpointSupplier) throws IOException {
        super(config, expectedTranslogUUID, deletionPolicy, Checkpoint.emptyTranslogCheckpoint(globalCheckpointSupplier),
            Checkpoint::read);
        this.globalCheckpointSupplier = globalCheckpointSupplier;
    }

    @Override
    protected TranslogWriter createWriter(ChannelFactory channelFactory, BaseCheckpoint checkpoint, FileChannel channel,
                                          Path path, ByteSizeValue bufferSize, LongSupplier minTranslogGenerationSupplier) throws IOException {
        return new TranslogWriter(channelFactory, shardId(), (Checkpoint) checkpoint, channel, path, bufferSize,
            () -> globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier);
    }

    @Override
    protected TranslogException createTranslogException(String reason, Exception cause) {
        return new TranslogException(shardId(), reason, cause);
    }

    @Override
    protected Location addToWriter(BaseTranslogWriter current, Writeable operation, BytesReference bytes) throws IOException {
        return ((TranslogWriter) current).add(bytes, ((Operation) operation).seqNo());
    }

    /**
     * Tests whether or not the translog should be flushed. This test is based on the current size
     * of the translog comparted to the configured flush threshold size.
     *
     * @return {@code true} if the translog should be flushed
     */
    public boolean shouldFlush() {
        final long size = this.uncommittedSizeInBytes();
        return size > indexSettings().getFlushThresholdSize().getBytes();
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test
     * is based on the size of the current generation compared to the configured generation
     * threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public boolean shouldRollGeneration() {
        final long size = this.current.sizeInBytes();
        final long threshold = indexSettings().getGenerationThresholdSize().getBytes();
        return size > threshold;
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    public long getLastSyncedGlobalCheckpoint() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return ((Checkpoint) current.getLastSyncedCheckpoint()).globalCheckpoint;
        }
    }

    /**
     * Returns the number of operations in the transaction files that contain operations with seq# above the given number.
     */
    public int estimateTotalOperationsFromMinSeq(long minSeqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return readersAboveMinSeqNo(minSeqNo).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    public Snapshot newSnapshotFromMinSeqNo(long minSeqNo) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            TranslogSnapshot[] snapshots = readersAboveMinSeqNo(minSeqNo).map(BaseTranslogReader::newSnapshot)
                .toArray(TranslogSnapshot[]::new);
            return newMultiSnapshot(snapshots);
        }
    }

    /**
     * Snapshots the current transaction log allowing to safely iterate over the snapshot.
     * Snapshots are fixed in time and will not be updated with future operations.
     */
    public Snapshot newSnapshot() throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            return newSnapshotFromGen(getMinFileGeneration());
        }
    }

    public Snapshot newSnapshotFromGen(long minGeneration) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (minGeneration < getMinFileGeneration()) {
                throw new IllegalArgumentException("requested snapshot generation [" + minGeneration + "] is not available. " +
                    "Min referenced generation is [" + getMinFileGeneration() + "]");
            }
            TranslogSnapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current))
                .filter(reader -> reader.getGeneration() >= minGeneration)
                .map(BaseTranslogReader::newSnapshot).toArray(TranslogSnapshot[]::new);
            return newMultiSnapshot(snapshots);
        }
    }

    protected Snapshot newMultiSnapshot(TranslogSnapshot[] snapshots) throws IOException {
        final Closeable onClose;
        if (snapshots.length == 0) {
            onClose = () -> {};
        } else {
            assert Arrays.stream(snapshots).map(BaseTranslogReader::getGeneration).min(Long::compareTo).get()
                == snapshots[0].generation : "first reader generation of " + snapshots + " is not the smallest";
            onClose = acquireTranslogGenFromDeletionPolicy(snapshots[0].generation);
        }
        boolean success = false;
        try {
            Snapshot result = new MultiSnapshot(snapshots, onClose);
            success = true;
            return result;
        } finally {
            if (success == false) {
                onClose.close();
            }
        }
    }

    /**
     * Gets the minimum generation that could contain any sequence number after the specified sequence number, or the current generation if
     * there is no generation that could any such sequence number.
     *
     * @param seqNo the sequence number
     * @return the minimum generation for the sequence number
     */
    public TranslogGeneration getMinGenerationForSeqNo(final long seqNo) {
        try (ReleasableLock ignored = writeLock.acquire()) {
            /*
             * When flushing, the engine will ask the translog for the minimum generation that could contain any sequence number after the
             * local checkpoint. Immediately after flushing, there will be no such generation, so this minimum generation in this case will
             * be the current translog generation as we do not need any prior generations to have a complete history up to the current local
             * checkpoint.
             */
            long minTranslogFileGeneration = this.currentFileGeneration();
            for (final TranslogReader reader : readers) {
                if (seqNo <= ((Checkpoint) reader.getCheckpoint()).maxSeqNo) {
                    minTranslogFileGeneration = Math.min(minTranslogFileGeneration, reader.getGeneration());
                }
            }
            return new TranslogGeneration(translogUUID, minTranslogFileGeneration);
        }
    }

    /**
     * Returns the size in bytes of the translog files with ops above the given seqNo
     */
    private long sizeOfGensAboveSeqNoInBytes(long minSeqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return readersAboveMinSeqNo(minSeqNo).mapToLong(BaseTranslogReader::sizeInBytes).sum();
        }
    }


    private Stream<? extends BaseTranslogReader> readersAboveMinSeqNo(long minSeqNo) {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread() :
            "callers of readersAboveMinSeqNo must hold a lock: readLock ["
                + readLock.isHeldByCurrentThread() + "], writeLock [" + readLock.isHeldByCurrentThread() + "]";
        return Stream.concat(readers.stream(), Stream.of(current))
            .filter(reader -> {
                final long maxSeqNo = ((Checkpoint) reader.getCheckpoint()).maxSeqNo;
                return maxSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || maxSeqNo >= minSeqNo;
            });
    }

    /**
     * Reads a list of operations written with {@link #writeOperations(StreamOutput, List)}
     */
    public static List<Operation> readOperations(StreamInput input) throws IOException {
        return readOperations(input, Operation::readOperation);
    }

    /**
     * Reads the sequence numbers global checkpoint from the translog checkpoint.
     *
     * @param location the location of the translog
     * @return the global checkpoint
     * @throws IOException if an I/O exception occurred reading the checkpoint
     */
    public static final long readGlobalCheckpoint(final Path location) throws IOException {
        return readCheckpoint(location).globalCheckpoint;
    }

    /** Reads and returns the current checkpoint */
    static final Checkpoint readCheckpoint(final Path location) throws IOException {
        return BaseCheckpoint.read(location.resolve(CHECKPOINT_FILE_NAME), Checkpoint::read);
    }

    @Override
    public TranslogConfig getConfig() {
        return ((TranslogConfig) super.getConfig());
    }

    @Override
    public ShardId shardId() {
        return getConfig().getShardId();
    }

    @Override
    public IndexSettings indexSettings() {
        return getConfig().getIndexSettings();
    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    public interface Operation extends Writeable {
        enum Type {
            @Deprecated
            CREATE((byte) 1),
            INDEX((byte) 2),
            DELETE((byte) 3),
            NO_OP((byte) 4);

            private final byte id;

            Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch (id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return INDEX;
                    case 3:
                        return DELETE;
                    case 4:
                        return NO_OP;
                    default:
                        throw new IllegalArgumentException("no type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();

        long seqNo();

        long primaryTerm();

        /**
         * Reads the type and the operation from the given stream. The operation must be written with
         * {@link Operation#writeOperation(StreamOutput, Operation)}
         */
        static Operation readOperation(final StreamInput input) throws IOException {
            final Operation.Type type = Translog.Operation.Type.fromId(input.readByte());
            switch (type) {
                case CREATE:
                    // the de-serialization logic in Index was identical to that of Create when create was deprecated
                case INDEX:
                    return new Index(input);
                case DELETE:
                    return new Delete(input);
                case NO_OP:
                    return new NoOp(input);
                default:
                    throw new AssertionError("no case for [" + type + "]");
            }
        }

        /**
         * Writes the type and translog operation to the given stream
         */
        static void writeOperation(final StreamOutput output, final Operation operation) throws IOException {
            output.writeByte(operation.opType().id());
            switch(operation.opType()) {
                case CREATE:
                    // the serialization logic in Index was identical to that of Create when create was deprecated
                case INDEX:
                    ((Index) operation).write(output);
                    break;
                case DELETE:
                    ((Delete) operation).write(output);
                    break;
                case NO_OP:
                    ((NoOp) operation).write(output);
                    break;
                default:
                    throw new AssertionError("no case for [" + operation.opType() + "]");
            }
        }

        @Override
        default void writeTo(StreamOutput out) throws IOException {
            writeOperation(out, this);
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    public interface Snapshot extends Closeable {

        /**
         * The total estimated number of operations in the snapshot.
         */
        int totalOperations();

        /**
         * The number of operations have been overridden (eg. superseded) in the snapshot so far.
         * If two operations have the same sequence number, the operation with a lower term will be overridden by the operation
         * with a higher term. Unlike {@link #totalOperations()}, this value is updated each time after {@link #next}) is called.
         */
        default int overriddenOperations() {
            return 0;
        }

        /**
         * Returns the next operation in the snapshot or <code>null</code> if we reached the end.
         */
        Operation next() throws IOException;

    }

    public static class Index implements Operation {

        public static final int FORMAT_2_X = 6; // since 2.0-beta1 and 1.1
        public static final int FORMAT_AUTO_GENERATED_IDS = FORMAT_2_X + 1; // since 5.0.0-beta1
        public static final int FORMAT_SEQ_NO = FORMAT_AUTO_GENERATED_IDS + 1; // since 6.0.0
        public static final int SERIALIZATION_FORMAT = FORMAT_SEQ_NO;

        private final String id;
        private final long autoGeneratedIdTimestamp;
        private final String type;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;
        private final VersionType versionType;
        private final BytesReference source;
        private final String routing;
        private final String parent;

        private Index(final StreamInput in) throws IOException {
            final int format = in.readVInt(); // SERIALIZATION_FORMAT
            assert format >= FORMAT_2_X : "format was: " + format;
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            routing = in.readOptionalString();
            parent = in.readOptionalString();
            this.version = in.readLong();
            if (format < FORMAT_SEQ_NO) {
                in.readLong(); // timestamp
                in.readLong(); // ttl
            }
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version) : "invalid version for writes: " + this.version;
            if (format >= FORMAT_AUTO_GENERATED_IDS) {
                this.autoGeneratedIdTimestamp = in.readLong();
            } else {
                this.autoGeneratedIdTimestamp = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
            }
            if (format >= FORMAT_SEQ_NO) {
                seqNo = in.readLong();
                primaryTerm = in.readLong();
            } else {
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                primaryTerm = 0;
            }
        }

        public Index(Engine.Index index, Engine.IndexResult indexResult) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.parent = index.parent();
            this.seqNo = indexResult.getSeqNo();
            this.primaryTerm = index.primaryTerm();
            this.version = indexResult.getVersion();
            this.versionType = index.versionType();
            this.autoGeneratedIdTimestamp = index.getAutoGeneratedIdTimestamp();
        }

        public Index(String type, String id, long seqNo, byte[] source) {
            this(type, id, seqNo, Versions.MATCH_ANY, VersionType.INTERNAL, source, null, null, -1);
        }

        public Index(String type, String id, long seqNo, long version, VersionType versionType, byte[] source, String routing,
                     String parent, long autoGeneratedIdTimestamp) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
            this.seqNo = seqNo;
            this.primaryTerm = 0;
            this.version = version;
            this.versionType = versionType;
            this.routing = routing;
            this.parent = parent;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public BytesReference source() {
            return this.source;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent);
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            out.writeOptionalString(routing);
            out.writeOptionalString(parent);
            out.writeLong(version);

            out.writeByte(versionType.getValue());
            out.writeLong(autoGeneratedIdTimestamp);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Index index = (Index) o;

            if (version != index.version ||
                    seqNo != index.seqNo ||
                    primaryTerm != index.primaryTerm ||
                    id.equals(index.id) == false ||
                    type.equals(index.type) == false ||
                    versionType != index.versionType ||
                    autoGeneratedIdTimestamp != index.autoGeneratedIdTimestamp ||
                    source.equals(index.source) == false) {
                    return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return !(parent != null ? !parent.equals(index.parent) : index.parent != null);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + Long.hashCode(autoGeneratedIdTimestamp);
            return result;
        }

        @Override
        public String toString() {
            return "Index{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                '}';
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

    }

    public static class Delete implements Operation {

        public static final int FORMAT_5_0 = 2; // 5.0 - 5.5
        private static final int FORMAT_SINGLE_TYPE = FORMAT_5_0 + 1; // 5.5 - 6.0
        private static final int FORMAT_SEQ_NO = FORMAT_SINGLE_TYPE + 1; // 6.0 - *
        public static final int SERIALIZATION_FORMAT = FORMAT_SEQ_NO;

        private final String type, id;
        private final Term uid;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;
        private final VersionType versionType;

        private Delete(final StreamInput in) throws IOException {
            final int format = in.readVInt();// SERIALIZATION_FORMAT
            assert format >= FORMAT_5_0 : "format was: " + format;
            if (format >= FORMAT_SINGLE_TYPE) {
                type = in.readString();
                id = in.readString();
                if (format >= FORMAT_SEQ_NO) {
                    uid = new Term(in.readString(), in.readBytesRef());
                } else {
                    uid = new Term(in.readString(), in.readString());
                }
            } else {
                uid = new Term(in.readString(), in.readString());
                // the uid was constructed from the type and id so we can
                // extract them back
                Uid uidObject = Uid.createUid(uid.text());
                type = uidObject.type();
                id = uidObject.id();
            }
            this.version = in.readLong();
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version);
            if (format >= FORMAT_SEQ_NO) {
                seqNo = in.readLong();
                primaryTerm = in.readLong();
            } else {
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                primaryTerm = 0;
            }
        }

        public Delete(Engine.Delete delete, Engine.DeleteResult deleteResult) {
            this(delete.type(), delete.id(), delete.uid(), deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getVersion(), delete.versionType());
        }

        /** utility for testing */
        public Delete(String type, String id, long seqNo, Term uid) {
            this(type, id, uid, seqNo, 0, Versions.MATCH_ANY, VersionType.INTERNAL);
        }

        public Delete(String type, String id, Term uid, long seqNo, long primaryTerm, long version, VersionType versionType) {
            this.type = Objects.requireNonNull(type);
            this.id = Objects.requireNonNull(id);
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return this.uid;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete operation");
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(type);
            out.writeString(id);
            out.writeString(uid.field());
            out.writeBytesRef(uid.bytes());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Delete delete = (Delete) o;

            return version == delete.version &&
                    seqNo == delete.seqNo &&
                    primaryTerm == delete.primaryTerm &&
                    uid.equals(delete.uid) &&
                    versionType == delete.versionType;
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" +
                "uid=" + uid +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                '}';
        }
    }

    public static class NoOp implements Operation {

        private final long seqNo;
        private final long primaryTerm;
        private final String reason;

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public String reason() {
            return reason;
        }

        private NoOp(final StreamInput in) throws IOException {
            seqNo = in.readLong();
            primaryTerm = in.readLong();
            reason = in.readString();
        }

        public NoOp(final long seqNo, final long primaryTerm, final String reason) {
            assert seqNo > SequenceNumbers.NO_OPS_PERFORMED;
            assert primaryTerm >= 0;
            assert reason != null;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reason = reason;
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
            out.writeString(reason);
        }

        @Override
        public Type opType() {
            return Type.NO_OP;
        }

        @Override
        public long estimateSize() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

        @Override
        public Source getSource() {
            throw new UnsupportedOperationException("source does not exist for a no-op");
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NoOp that = (NoOp) obj;
            return seqNo == that.seqNo && primaryTerm == that.primaryTerm && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return 31 * 31 * 31 + 31 * 31 * Long.hashCode(seqNo) + 31 * Long.hashCode(primaryTerm) + reason().hashCode();
        }

        @Override
        public String toString() {
            return "NoOp{" +
                "seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", reason='" + reason + '\'' +
                '}';
        }
    }

    public static class Source {

        public final BytesReference source;
        public final String routing;
        public final String parent;

        public Source(BytesReference source, String routing, String parent) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
        }

    }
}

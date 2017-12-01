package org.elasticsearch.index.translog;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.LongSupplier;

public class Checkpoint extends BaseCheckpoint {

    final long minSeqNo;
    final long maxSeqNo;
    final long globalCheckpoint;

    static final int FILE_SIZE = BaseCheckpoint.FILE_SIZE
        + Long.BYTES // minimum sequence number, introduced in 6.0.0
        + Long.BYTES // maximum sequence number, introduced in 6.0.0
        + Long.BYTES; // global checkpoint, introduced in 6.0.0

    /**
     * Create a new translog checkpoint.
     *
     * @param offset           the current offset in the translog
     * @param numOps           the current number of operations in the translog
     * @param generation       the current translog generation
     * @param minSeqNo         the current minimum sequence number of all operations in the translog
     * @param maxSeqNo         the current maximum sequence number of all operations in the translog
     * @param globalCheckpoint the last-known global checkpoint
     * @param minTranslogGeneration the minimum generation referenced by the translog at this moment.
     */
    Checkpoint(long offset, int numOps, long generation, long minSeqNo, long maxSeqNo, long globalCheckpoint,
               long minTranslogGeneration) {
        super(offset, numOps, generation, minTranslogGeneration);
        assert minSeqNo <= maxSeqNo : "minSeqNo [" + minSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.globalCheckpoint = globalCheckpoint;
    }

    public static Checkpoint read(Path path) throws IOException {
        return BaseCheckpoint.read(path, Checkpoint::read);
    }

    public static Checkpoint read(DataInput in) throws IOException {
        long offset = in.readLong();
        int numOps = in.readInt();
        long generation = in.readLong();
        long minSeqNo = in.readLong();
        long maxSeqNo = in.readLong();
        long globalCheckpoint = in.readLong();
        long minTranslogGeneration = in.readLong();
        return new Checkpoint(offset, numOps, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration);
    }

    @Override
    protected int fileSize() {
        return FILE_SIZE;
    }

    @Override
    protected void writeAdditionalFields(DataOutput out) throws IOException {
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(globalCheckpoint);
    }

    static Checkpoint emptyTranslogCheckpoint(final long offset, final long generation, final long globalCheckpoint,
                                              long minTranslogGeneration) {
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        return new Checkpoint(offset, 0, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration);
    }

    public static InitialCheckpointSupplier emptyTranslogCheckpoint(LongSupplier globalCheckpointSupplier) {
        return (offset, generation, minTranslogGeneration) ->
            emptyTranslogCheckpoint(offset, generation, globalCheckpointSupplier.getAsLong(), minTranslogGeneration);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
            "offset=" + offset +
            ", numOps=" + numOps +
            ", generation=" + generation +
            ", minSeqNo=" + minSeqNo +
            ", maxSeqNo=" + maxSeqNo +
            ", globalCheckpoint=" + globalCheckpoint +
            ", minTranslogGeneration=" + minTranslogGeneration +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (offset != that.offset) return false;
        if (numOps != that.numOps) return false;
        if (generation != that.generation) return false;
        if (minSeqNo != that.minSeqNo) return false;
        if (maxSeqNo != that.maxSeqNo) return false;
        return globalCheckpoint == that.globalCheckpoint;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        result = 31 * result + Long.hashCode(minSeqNo);
        result = 31 * result + Long.hashCode(maxSeqNo);
        result = 31 * result + Long.hashCode(globalCheckpoint);
        return result;
    }
}

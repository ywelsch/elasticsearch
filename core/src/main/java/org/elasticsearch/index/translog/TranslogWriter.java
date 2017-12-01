package org.elasticsearch.index.translog;

import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Operation;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

public class TranslogWriter extends BaseTranslogWriter {

    private final LongSupplier globalCheckpointSupplier;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;
    private final ShardId shardId;

    TranslogWriter(ChannelFactory channelFactory, ShardId shardId, Checkpoint initialCheckpoint,
                   FileChannel channel,
                   Path path, ByteSizeValue bufferSize,
                   LongSupplier globalCheckpointSupplier,
                   LongSupplier minTranslogGenerationSupplier) throws IOException {
        super(channelFactory, initialCheckpoint, channel, path, bufferSize, minTranslogGenerationSupplier);
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.shardId = shardId;
    }

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write
     * @param seqNo the sequence number associated with the operation
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public synchronized BaseTranslog.Location add(final BytesReference data, final long seqNo) throws IOException {
        BaseTranslog.Location location = add(data);
        if (minSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 1;
        }
        if (maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 1;
        }

        minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
        maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);

        assert assertNoSeqNumberConflict(seqNo, data);
        return location;
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Operation newOp = BaseTranslog.readOperation(new BufferedChecksumStreamInput(data.streamInput()), Operation::readOperation);
                Operation prvOp = BaseTranslog.readOperation(new BufferedChecksumStreamInput(previous.v1().streamInput()),
                    Operation::readOperation);
                throw new AssertionError(
                    "seqNo [" + seqNo + "] was processed twice in generation [" + generation + "], with different data. " +
                        "prvOp [" + prvOp + "], newOp [" + newOp + "]", previous.v2());
            }
        } else {
            seenSequenceNumbers.put(seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(totalOffset, operationCounter, generation, minSeqNo, maxSeqNo,
            globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong());
    }

    @Override
    public boolean syncNeeded() {
        return super.syncNeeded() ||
            globalCheckpointSupplier.getAsLong() != ((Checkpoint) lastSyncedCheckpoint).globalCheckpoint;
    }

    @Override
    protected TranslogException createTranslogException(String reason, Exception cause) {
        return new TranslogException(shardId, reason, cause);
    }
}

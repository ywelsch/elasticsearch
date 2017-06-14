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
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class PrimaryReplicaSyncer extends AbstractComponent {

    private final TaskManager taskManager;
    private final SyncAction syncAction;

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    @Inject
    public PrimaryReplicaSyncer(Settings settings, TransportService transportService, TransportResyncReplicationAction syncAction) {
        this(settings, transportService.getTaskManager(), (SyncAction) syncAction);
    }

    // for tests
    public PrimaryReplicaSyncer(Settings settings, TaskManager taskManager, SyncAction syncAction) {
        super(settings);
        this.taskManager = taskManager;
        this.syncAction = syncAction;
    }

    void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void resync(IndexShard indexShard, ActionListener<ResyncTask> listener) {
        ResyncRequest request = new ResyncRequest(indexShard.shardId());
        ResyncTask resyncTask = (ResyncTask) taskManager.register("transport", "resync", request); // it's not transport :-)
        final Translog.View translogView;
        try {
            translogView = indexShard.acquireTranslogView();
        } catch (Exception e) {
            resyncTask.setPhase("failed");
            taskManager.unregister(resyncTask);
            listener.onFailure(e);
            return;
        }
        ActionListener<Void> wrappedListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void ignore) {
                try {
                    translogView.close();
                    resyncTask.setPhase("finished");
                    taskManager.unregister(resyncTask);
                    listener.onResponse(resyncTask);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    translogView.close();
                } catch (IOException inner) {
                    e.addSuppressed(inner);
                }
                resyncTask.setPhase("finished");
                taskManager.unregister(resyncTask);
                listener.onFailure(e);
            }
        };
        try {
            Translog.Snapshot snapshot = translogView.snapshot();
            new SnapshotSender(logger, syncAction, resyncTask, indexShard, snapshot, chunkSize.bytesAsInt(),
                indexShard.getGlobalCheckpoint() + 1, indexShard.seqNoStats().getMaxSeqNo(), wrappedListener).run();
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }

    public interface SyncAction {
        void sync(ResyncReplicationRequest request, Task parentTask, ActionListener<ResyncReplicationResponse> listener);
    }

    static class SnapshotSender extends AbstractRunnable implements ActionListener<ResyncReplicationResponse> {
        private final Logger logger;
        private final SyncAction syncAction;
        private final ResyncTask task; // to track progress
        private final IndexShard indexShard;
        private final Translog.Snapshot snapshot;
        private final long startingSeqNo;
        private final long endingSeqNo;
        private final int chunkSizeInBytes;
        private final ActionListener<Void> listener;
        private final AtomicInteger totalSentOps = new AtomicInteger();
        private final AtomicInteger totalSkippedOps = new AtomicInteger();
        private AtomicBoolean closed = new AtomicBoolean();
        private AtomicBoolean runOnce = new AtomicBoolean();

        SnapshotSender(Logger logger, SyncAction syncAction, ResyncTask task, IndexShard indexShard, Translog.Snapshot snapshot,
                       int chunkSizeInBytes, long startingSeqNo, long endingSeqNo, ActionListener<Void> listener) {
            this.logger = logger;
            this.syncAction = syncAction;
            this.task = task;
            this.indexShard = indexShard;
            this.snapshot = snapshot;
            this.chunkSizeInBytes = chunkSizeInBytes;
            this.startingSeqNo = startingSeqNo;
            this.endingSeqNo = endingSeqNo;
            this.listener = listener;
            task.setTotalOperations(snapshot.totalOperations());
        }

        @Override
        public void onResponse(ResyncReplicationResponse response) {
            run();
        }

        @Override
        public void onFailure(Exception e) {
            if (closed.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        // method is synchronized as this method is called by different threads. Even though those calls are not concurrent,
        // snapshot.next() uses non-synchronized state and is not multi-thread-compatible
        @Override
        protected synchronized void doRun() throws Exception {
            long size = 0;
            final List<Translog.Operation> operations = new ArrayList<>();

            task.setPhase("syncing");
            task.setResyncedOperations(totalSentOps.get());
            task.setSkippedOperations(totalSkippedOps.get());

            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (indexShard.state() != IndexShardState.STARTED) {
                    throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
                }
                final long seqNo = operation.seqNo();
                if (startingSeqNo >= 0 &&
                    (seqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo || seqNo > endingSeqNo)) {
                    totalSkippedOps.incrementAndGet();
                    continue;
                }
                operations.add(operation);
                size += operation.estimateSize();
                totalSentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (size >= chunkSizeInBytes) {
                    break;
                }
            }

            if (runOnce.compareAndSet(false, true) || !operations.isEmpty()) {
                ResyncReplicationRequest request = new ResyncReplicationRequest(
                    indexShard.routingEntry().allocationId().getId(), indexShard.shardId(), operations);
                syncAction.sync(request, task, this);
                logger.trace("sending batch of [{}][{}] (total sent: [{}], skipped: [{}])", operations.size(), new ByteSizeValue(size),
                    totalSentOps.get(), totalSkippedOps.get());
            } else if (closed.compareAndSet(false, true)) {
                logger.trace("resync completed (total sent: [{}], skipped: [{}])", totalSentOps.get(), totalSkippedOps.get());
                listener.onResponse(null);
            }
        }
    }

    public static class ResyncRequest extends ActionRequest {

        private final ShardId shardId;

        public ResyncRequest(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new ResyncTask(id, type, action, getDescription(), parentTaskId);
        }

        @Override
        public String getDescription() {
            return toString();
        }

        @Override
        public String toString() {
            return "ResyncRequest{" + shardId + "}";
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class ResyncTask extends Task {
        private volatile String phase = "starting";
        private volatile int totalOperations;
        private volatile int resyncedOperations;
        private volatile int skippedOperations;

        public ResyncTask(long id, String type, String action, String description, TaskId parentTaskId) {
            super(id, type, action, description, parentTaskId);
        }

        /**
         * Set the current phase of the task.
         */
        public void setPhase(String phase) {
            this.phase = phase;
        }

        /**
         * Get the current phase of the task.
         */
        public String getPhase() {
            return phase;
        }

        /**
         * total number of translog operations that were captured by translog snapshot
         */
        public int getTotalOperations() {
            return totalOperations;
        }

        public void setTotalOperations(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        /**
         * number of operations that have been successfully replicated
         */
        public int getResyncedOperations() {
            return resyncedOperations;
        }

        public void setResyncedOperations(int resyncedOperations) {
            this.resyncedOperations = resyncedOperations;
        }

        /**
         * number of translog operations that have been skipped
         */
        public int getSkippedOperations() {
            return skippedOperations;
        }

        public void setSkippedOperations(int skippedOperations) {
            this.skippedOperations = skippedOperations;
        }

        @Override
        public ResyncTask.Status getStatus() {
            return new ResyncTask.Status(phase, totalOperations, resyncedOperations, skippedOperations);
        }

        public static class Status implements Task.Status {
            public static final String NAME = "resync";

            private final String phase;
            private final int totalOperations;
            private final int resyncedOperations;
            private final int skippedOperations;

            public Status(StreamInput in) throws IOException {
                phase = in.readString();
                totalOperations = in.readVInt();
                resyncedOperations = in.readVInt();
                skippedOperations = in.readVInt();
            }

            public Status(String phase, int totalOperations, int resyncedOperations, int skippedOperations) {
                this.phase = requireNonNull(phase, "Phase cannot be null");
                this.totalOperations = totalOperations;
                this.resyncedOperations = resyncedOperations;
                this.skippedOperations = skippedOperations;
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("phase", phase);
                builder.field("totalOperations", totalOperations);
                builder.field("resyncedOperations", resyncedOperations);
                builder.field("skippedOperations", skippedOperations);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(phase);
                out.writeVLong(totalOperations);
                out.writeVLong(resyncedOperations);
                out.writeVLong(skippedOperations);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }


            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Status status = (Status) o;

                if (totalOperations != status.totalOperations) return false;
                if (resyncedOperations != status.resyncedOperations) return false;
                if (skippedOperations != status.skippedOperations) return false;
                return phase.equals(status.phase);
            }

            @Override
            public int hashCode() {
                int result = phase.hashCode();
                result = 31 * result + totalOperations;
                result = 31 * result + resyncedOperations;
                result = 31 * result + skippedOperations;
                return result;
            }
        }
    }
}

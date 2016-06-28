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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.SuspendableRefContainer;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IndexShardOperationsLock {
    private final ESLogger logger;
    private final ThreadPool threadPool;

    private final SuspendableRefContainer suspendableRefContainer = new SuspendableRefContainer();
    private final Object mutex = new Object(); // mutex used to protect the following two fields
    private @Nullable List<ActionListener<Releasable>> delayedOperations; // operations that are delayed due to relocation hand-off
    private boolean blockOperationsInProgress; // only allow one thread to try blocking operations at a time

    public IndexShardOperationsLock(ESLogger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    /**
     * Wait for in-flight operations to finish and executes onBlocked under the guarantee that no new operations are started. Queues
     * operations that are occurring in the meanwhile and runs them once onBlocked has executed.
     *
     * @param timeout the maximum time to wait for the in-flight operations block
     * @param timeUnit the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @throws IllegalStateException if operation is called while running
     * @throws InterruptedException if calling thread is interrupted
     * @throws TimeoutException if timed out waiting for in-flight operations to finish
     */
    public void blockOperations(long timeout, TimeUnit timeUnit, Runnable onBlocked) throws InterruptedException, TimeoutException {
        synchronized (mutex) {
            if (blockOperationsInProgress) {
                throw new IllegalStateException("cannot invoke blockOperations while it's running already");
            }
            blockOperationsInProgress = true;
        }
        try {
            try (Releasable block = suspendableRefContainer.tryBlockAcquisition(timeout, timeUnit)) {
                onBlocked.run();
            }
        } finally {
            final List<ActionListener<Releasable>> queuedActions;
            synchronized (mutex) {
                assert blockOperationsInProgress;
                blockOperationsInProgress = false;
                queuedActions = delayedOperations;
                delayedOperations = null;
            }
            if (queuedActions != null) {
                for (ActionListener<Releasable> queuedAction : queuedActions) {
                    acquire(queuedAction, null, false);
                }
            }
        }
    }

    /**
     * Acquires a lock whenever lock acquisition is not blocked. If the lock is directly available, the provided
     * ActionListener will be called on the calling thread. During calls of {@link #blockOperations(long, TimeUnit, Runnable)}, lock
     * acquisition can be delayed. The provided ActionListener will then be called using the provided executor once blockOperations
     * terminates.
     *
     * @param onAcquired ActionListener that is invoked once acquisition is successful or failed
     * @param executor executor to use for delayed call
     * @param forceExecution whether the runnable should force its execution in case it gets rejected
     */
    public void acquire(ActionListener<Releasable> onAcquired, String executor, boolean forceExecution) {
        while (true) {
            final Releasable releasable;
            try {
                releasable = suspendableRefContainer.tryAcquire();
            } catch (InterruptedException e) {
                onAcquired.onFailure(e);
                return;
            }
            if (releasable != null) {
                onAcquired.onResponse(releasable);
                return;
            } else {
                // relocation hand-off in progress, store onRelocated callback to trigger after hand-off finished
                synchronized (mutex) {
                    if (blockOperationsInProgress) {
                        // we know this will be released by call to releaseDelayedOperations
                        if (delayedOperations == null) {
                            delayedOperations = new ArrayList<>();
                        }
                        if (executor != null) {
                            delayedOperations.add(new ThreadedActionListener(logger, threadPool, executor, onAcquired, forceExecution));
                        } else {
                            delayedOperations.add(onAcquired);
                        }
                        return;
                    }
                }
                // blockOperations was finished just after we tried to acquire operation lock and before we acquired mutex
                // give it another try
            }
        }
    }

    public int getActiveOperationsCount() {
        return suspendableRefContainer.activeRefs();
    }
}

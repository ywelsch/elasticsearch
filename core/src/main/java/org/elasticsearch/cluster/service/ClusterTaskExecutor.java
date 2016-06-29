package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class ClusterTaskExecutor implements Releasable {

    private final PrioritizedEsThreadPoolExecutor updateTasksExecutor;

    public ClusterTaskExecutor(String threadName, Settings settings, ThreadPool threadPool) {
        this.updateTasksExecutor = EsExecutors.newSinglePrioritizing(threadName, daemonThreadFactory(settings, threadName),
            threadPool.getThreadContext());
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        PrioritizedEsThreadPoolExecutor.Pending[] pendings = updateTasksExecutor.getPending();
        List<PendingClusterTask> pendingClusterTasks = new ArrayList<>(pendings.length);
        for (PrioritizedEsThreadPoolExecutor.Pending pending : pendings) {
            final String source;
            final long timeInQueue;
            // we have to capture the task as it will be nulled after execution and we don't want to change while we check things here.
            final Object task = pending.task;
            if (task == null) {
                continue;
            } else if (task instanceof SourcePrioritizedRunnable) {
                SourcePrioritizedRunnable runnable = (SourcePrioritizedRunnable) task;
                source = runnable.source();
                timeInQueue = runnable.getAgeInMillis();
            } else {
                assert false : "expected SourcePrioritizedRunnable got " + task.getClass();
                source = "unknown [" + task.getClass() + "]";
                timeInQueue = 0;
            }

            pendingClusterTasks.add(
                new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(source), timeInQueue, pending.executing));
        }
        return pendingClusterTasks;
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return updateTasksExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return updateTasksExecutor.getMaxTaskWaitTime();
    }

    @Override
    public void close() {
        ThreadPool.terminate(updateTasksExecutor, 10, TimeUnit.SECONDS);
    }

    public void execute(SourcePrioritizedRunnable runnable) {
        updateTasksExecutor.execute(runnable);
    }

    public void execute(SourcePrioritizedRunnable runnable, ScheduledExecutorService scheduler, TimeValue timeout,
                            Runnable timeoutCallback) {
        updateTasksExecutor.execute(runnable, scheduler, timeout, timeoutCallback);
    }


    public static abstract class SourcePrioritizedRunnable extends PrioritizedRunnable {
        protected final String source;

        public SourcePrioritizedRunnable(Priority priority, String source) {
            super(priority);
            this.source = source;
        }

        public String source() {
            return source;
        }
    }
}

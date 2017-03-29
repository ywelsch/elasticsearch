package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.core.Is.is;

public class BatchingClusterTaskExecutorTests extends ESTestCase {

    interface FailureListener {
        void onFailure(String source, Exception e);

        default void processed(String source) {

        }
    }

    interface Executor<T> extends BatchingClusterTaskExecutor.BatchingExecutor<T> {
        void execute(List<T> tasks);
    }

    static <T> BatchingClusterTaskExecutor.RunTasks<T, FailureListener, Executor<T>> runTasks() {
        return (executor, toExecute, tasksSummary) -> {
            executor.execute(toExecute.stream().map(t -> t.task).collect(Collectors.toList()));
            toExecute.forEach(updateTask -> updateTask.listener.processed(updateTask.source));
        };
    }

    static abstract class SingleTask implements Executor<SingleTask>, FailureListener, ClusterStateTaskConfig {

        @Override
        public void execute(List<SingleTask> tasks) {
            tasks.forEach(SingleTask::run);
        }

        @Nullable
        @Override
        public TimeValue timeout() {
            return null;
        }

        @Override
        public Priority priority() {
            return Priority.NORMAL;
        }

        public abstract void run();
    }



    protected static ThreadPool threadPool;
    protected BatchingClusterTaskExecutor<FailureListener> clusterTaskExecutor;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(BatchingClusterTaskExecutorTests.class.getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterTaskExecutor = new BatchingClusterTaskExecutor<FailureListener>(logger, EsExecutors.newSinglePrioritizing("test_thread",
            daemonThreadFactory(Settings.EMPTY, "test_thread"), threadPool.getThreadContext()), threadPool) {

            @Override
            protected void onTimeout(String source, FailureListener listener, TimeValue timeout) {
                listener.onFailure(source, new TimeoutException());
            }
        };
    }

    @After
    public void shutDownThreadExecutor() {
        ThreadPool.terminate(clusterTaskExecutor.threadExecutor, 10, TimeUnit.SECONDS);
    }

    public void testTimedOutUpdateTaskCleanedUp() throws Exception {
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch blockCompleted = new CountDownLatch(1);
        SingleTask blockTask = new SingleTask() {

            @Override
            public void run() {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                blockCompleted.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }
        };
        clusterTaskExecutor.submitTask("block-task", blockTask, blockTask, blockTask, blockTask, runTasks());

        final CountDownLatch block2 = new CountDownLatch(1);
        SingleTask unblockTask = new SingleTask() {

            @Override
            public void run() {
                block2.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                block2.countDown();
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.ZERO;
            }
        };
        clusterTaskExecutor.submitTask("unblock-task", unblockTask, unblockTask, unblockTask, unblockTask, runTasks());

        block.countDown();
        block2.await();
        blockCompleted.await();
        synchronized (clusterTaskExecutor.updateTasksPerExecutor) {
            assertTrue("expected empty map but was " + clusterTaskExecutor.updateTasksPerExecutor,
                clusterTaskExecutor.updateTasksPerExecutor.isEmpty());
        }
    }

    public void testTimeoutUpdateTask() throws Exception {
        final CountDownLatch block = new CountDownLatch(1);
        SingleTask test1 = new SingleTask() {
            @Override
            public void run() {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }
        };
        clusterTaskExecutor.submitTask("block-task", test1, test1, test1, test1, runTasks());

        final CountDownLatch timedOut = new CountDownLatch(1);
        final AtomicBoolean executeCalled = new AtomicBoolean();
        SingleTask test2 = new SingleTask() {

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(2);
            }

            @Override
            public void run() {
                executeCalled.set(true);
            }

            @Override
            public void onFailure(String source, Exception e) {
                timedOut.countDown();
            }
        };
        clusterTaskExecutor.submitTask("block-task", test2, test2, test2, test2, runTasks());

        timedOut.await();
        block.countDown();
        final CountDownLatch allProcessed = new CountDownLatch(1);
        SingleTask test3 = new SingleTask() {

            @Override
            public void run() {
                allProcessed.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }
        };
        clusterTaskExecutor.submitTask("block-task", test3, test3, test3, test3, runTasks());
        allProcessed.await(); // executed another task to double check that execute on the timed out update task is not called...
        assertThat(executeCalled.get(), equalTo(false));
    }

    public void testOneExecutorDontStarveAnother() throws InterruptedException {
        final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        final Semaphore allowProcessing = new Semaphore(0);
        final Semaphore startedProcessing = new Semaphore(0);

        class TaskExecutor implements Executor<String> {

            @Override
            public void execute(List<String> tasks) {
                executionOrder.addAll(tasks); // do this first, so startedProcessing can be used as a notification that this is done.
                startedProcessing.release(tasks.size());
                try {
                    allowProcessing.acquire(tasks.size());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        TaskExecutor executorA = new TaskExecutor();
        TaskExecutor executorB = new TaskExecutor();

        final ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.NORMAL);
        final FailureListener noopListener = (source, e) -> {
            throw new AssertionError(e);
        };
        // this blocks the cluster state queue, so we can set it up right
        clusterTaskExecutor.submitTask("0", "A0", config, executorA, noopListener, runTasks());
        // wait to be processed
        startedProcessing.acquire(1);
        assertThat(executionOrder, equalTo(Arrays.asList("A0")));


        // these will be the first batch
        clusterTaskExecutor.submitTask("1", "A1", config, executorA, noopListener, runTasks());
        clusterTaskExecutor.submitTask("2", "A2", config, executorA, noopListener, runTasks());

        // release the first 0 task, but not the second
        allowProcessing.release(1);
        startedProcessing.acquire(2);
        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2")));

        // setup the queue with pending tasks for another executor same priority
        clusterTaskExecutor.submitTask("3", "B3", config, executorB, noopListener, runTasks());
        clusterTaskExecutor.submitTask("4", "B4", config, executorB, noopListener, runTasks());


        clusterTaskExecutor.submitTask("5", "A5", config, executorA, noopListener, runTasks());
        clusterTaskExecutor.submitTask("6", "A6", config, executorA, noopListener, runTasks());

        // now release the processing
        allowProcessing.release(6);

        // wait for last task to be processed
        startedProcessing.acquire(4);

        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2", "B3", "B4", "A5", "A6")));

    }

    static class TaskExecutor implements Executor<Integer> {
        List<Integer> tasks = new ArrayList<>();

        @Override
        public void execute(List<Integer> tasks) {
            this.tasks.addAll(tasks);
        }
    }

    // test that for a single thread, tasks are executed in the order
    // that they are submitted
    public void testTasksAreExecutedInOrder() throws BrokenBarrierException, InterruptedException {


        int numberOfThreads = randomIntBetween(2, 8);
        TaskExecutor[] executors = new TaskExecutor[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            executors[i] = new TaskExecutor();
        }

        int tasksSubmittedPerThread = randomIntBetween(2, 1024);

        CopyOnWriteArrayList<Tuple<String, Throwable>> failures = new CopyOnWriteArrayList<>();
        CountDownLatch updateLatch = new CountDownLatch(numberOfThreads * tasksSubmittedPerThread);

        final FailureListener listener = new FailureListener() {
            @Override
            public void onFailure(String source, Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure: [{}]", source), e);
                failures.add(new Tuple<>(source, e));
                updateLatch.countDown();
            }

            @Override
            public void processed(String source) {
                updateLatch.countDown();
            }
        };

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < tasksSubmittedPerThread; j++) {
                        clusterTaskExecutor.submitTask("[" + index + "][" + j + "]", j,
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())), executors[index], listener, runTasks());
                    }
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();

        updateLatch.await();

        assertThat(failures, empty());

        for (int i = 0; i < numberOfThreads; i++) {
            assertEquals(tasksSubmittedPerThread, executors[i].tasks.size());
            for (int j = 0; j < tasksSubmittedPerThread; j++) {
                assertNotNull(executors[i].tasks.get(j));
                assertEquals("cluster state update task executed out of order", j, (int) executors[i].tasks.get(j));
            }
        }
    }

    public void testSingleBatchSubmission() throws InterruptedException {
        Map<Integer, FailureListener> tasks = new HashMap<>();
        final int numOfTasks = randomInt(10);
        final CountDownLatch latch = new CountDownLatch(numOfTasks);
        for (int i = 0; i < numOfTasks; i++) {
            while (null != tasks.put(randomInt(1024), new FailureListener() {
                @Override
                public void processed(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail(ExceptionsHelper.detailedMessage(e));
                }
            })) ;
        }

        Executor<Integer> executor = taskList -> {
            assertThat(taskList.size(), equalTo(tasks.size()));
            assertThat(taskList.stream().collect(Collectors.toSet()), equalTo(tasks.keySet()));
        };
        clusterTaskExecutor.submitTasks("test", tasks, ClusterStateTaskConfig.build(Priority.LANGUID), executor, runTasks());

        latch.await();
    }

    /**
     * Note, this test can only work as long as we have a single thread executor executing the state update tasks!
     */
    public void testPrioritizedTasks() throws Exception {
        BlockingTask block = new BlockingTask(Priority.IMMEDIATE);
        clusterTaskExecutor.submitTask("test", block, block, block, block, runTasks());
        int taskCount = randomIntBetween(5, 20);

        // will hold all the tasks in the order in which they were executed
        List<PrioritizedTask> tasks = new ArrayList<>(taskCount);
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            Priority priority = randomFrom(Priority.values());
            PrioritizedTask task = new PrioritizedTask(priority, latch, tasks);
            clusterTaskExecutor.submitTask("test", task, task, task, task, runTasks());
        }

        block.close();
        latch.await();

        Priority prevPriority = null;
        for (PrioritizedTask task : tasks) {
            if (prevPriority == null) {
                prevPriority = task.priority();
            } else {
                assertThat(task.priority().sameOrAfter(prevPriority), is(true));
            }
        }
    }

    public void testDuplicateSubmission() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        try (BlockingTask blockingTask = new BlockingTask(Priority.IMMEDIATE)) {
            clusterTaskExecutor.submitTask("blocking", blockingTask, blockingTask, blockingTask, blockingTask, runTasks());

            Executor<SimpleTask> executor = tasks -> {};
            SimpleTask task = new SimpleTask(1);
            FailureListener listener = new FailureListener() {
                @Override
                public void processed(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail(ExceptionsHelper.detailedMessage(e));
                }
            };

            clusterTaskExecutor.submitTask("first time", task, ClusterStateTaskConfig.build(Priority.NORMAL), executor,
                listener, runTasks());

            final IllegalStateException e =
                expectThrows(
                    IllegalStateException.class,
                    () -> clusterTaskExecutor.submitTask(
                        "second time",
                        task,
                        ClusterStateTaskConfig.build(Priority.NORMAL),
                        executor, listener, runTasks()));
            assertThat(e, hasToString(containsString("task [1] with source [second time] is already queued")));

            clusterTaskExecutor.submitTask("third time a charm", new SimpleTask(1),
                ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener, runTasks());

            assertThat(latch.getCount(), equalTo(2L));
        }
        latch.await();
    }

    private static class SimpleTask {
        private final int id;

        private SimpleTask(int id) {
            this.id = id;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }

    private static class BlockingTask extends SingleTask implements Releasable {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Priority priority;

        public BlockingTask(Priority priority) {
            super();
            this.priority = priority;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onFailure(String source, Exception e) {
        }

        @Override
        public Priority priority() {
            return priority;
        }

        public void close() {
            latch.countDown();
        }

    }

    private static class PrioritizedTask extends SingleTask {

        private final CountDownLatch latch;
        private final List<PrioritizedTask> tasks;
        private final Priority priority;

        private PrioritizedTask(Priority priority, CountDownLatch latch, List<PrioritizedTask> tasks) {
            super();
            this.latch = latch;
            this.tasks = tasks;
            this.priority = priority;
        }

        @Override
        public void run() {
            tasks.add(this);
            latch.countDown();
        }

        @Override
        public Priority priority() {
            return priority;
        }

        @Override
        public void onFailure(String source, Exception e) {
            latch.countDown();
        }
    }

}

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

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.BaseFuture;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public abstract class AbstractClusterTaskExecutorTestCase<T extends AbstractClusterTaskExecutor> extends ESTestCase {

    protected static ThreadPool threadPool;
    protected T clusterTaskExecutor;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(AbstractClusterTaskExecutorTestCase.class.getName());
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
        clusterTaskExecutor = createClusterTaskExecutor();
    }

    @After
    public void tearDown() throws Exception {
        clusterTaskExecutor.close();
        super.tearDown();
    }

    protected abstract T createClusterTaskExecutor() throws InterruptedException;

    public void testTimedOutUpdateTaskCleanedUp() throws Exception {
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch blockCompleted = new CountDownLatch(1);
        clusterTaskExecutor.submitStateUpdateTask("block-task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                blockCompleted.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }
        });

        final CountDownLatch block2 = new CountDownLatch(1);
        clusterTaskExecutor.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                block2.countDown();
                return currentState;
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.ZERO;
            }

            @Override
            public void onFailure(String source, Exception e) {
                block2.countDown();
            }
        });
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
        clusterTaskExecutor.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }
        });

        final CountDownLatch timedOut = new CountDownLatch(1);
        final AtomicBoolean executeCalled = new AtomicBoolean();
        clusterTaskExecutor.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(2);
            }

            @Override
            public void onFailure(String source, Exception e) {
                timedOut.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                executeCalled.set(true);
                return currentState;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });

        timedOut.await();
        block.countDown();
        final CountDownLatch allProcessed = new CountDownLatch(1);
        clusterTaskExecutor.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                allProcessed.countDown();
                return currentState;
            }

        });
        allProcessed.await(); // executed another task to double check that execute on the timed out update task is not called...
        assertThat(executeCalled.get(), equalTo(false));
    }

    public void testOneExecutorDontStarveAnother() throws InterruptedException {
        final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        final Semaphore allowProcessing = new Semaphore(0);
        final Semaphore startedProcessing = new Semaphore(0);

        class TaskExecutor implements ClusterStateTaskExecutor<String> {

            @Override
            public ClusterTasksResult<String> execute(ClusterState currentState, List<String> tasks) throws Exception {
                executionOrder.addAll(tasks); // do this first, so startedProcessing can be used as a notification that this is done.
                startedProcessing.release(tasks.size());
                allowProcessing.acquire(tasks.size());
                return ClusterTasksResult.<String>builder().successes(tasks).build(currentState);
            }
        }

        TaskExecutor executorA = new TaskExecutor();
        TaskExecutor executorB = new TaskExecutor();

        final ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.NORMAL);
        final ClusterStateTaskListener noopListener = (source, e) -> { throw new AssertionError(source, e); };
        // this blocks the cluster state queue, so we can set it up right
        clusterTaskExecutor.submitStateUpdateTask("0", "A0", config, executorA, noopListener);
        // wait to be processed
        startedProcessing.acquire(1);
        assertThat(executionOrder, equalTo(Arrays.asList("A0")));


        // these will be the first batch
        clusterTaskExecutor.submitStateUpdateTask("1", "A1", config, executorA, noopListener);
        clusterTaskExecutor.submitStateUpdateTask("2", "A2", config, executorA, noopListener);

        // release the first 0 task, but not the second
        allowProcessing.release(1);
        startedProcessing.acquire(2);
        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2")));

        // setup the queue with pending tasks for another executor same priority
        clusterTaskExecutor.submitStateUpdateTask("3", "B3", config, executorB, noopListener);
        clusterTaskExecutor.submitStateUpdateTask("4", "B4", config, executorB, noopListener);


        clusterTaskExecutor.submitStateUpdateTask("5", "A5", config, executorA, noopListener);
        clusterTaskExecutor.submitStateUpdateTask("6", "A6", config, executorA, noopListener);

        // now release the processing
        allowProcessing.release(6);

        // wait for last task to be processed
        startedProcessing.acquire(4);

        assertThat(executionOrder, equalTo(Arrays.asList("A0", "A1", "A2", "B3", "B4", "A5", "A6")));

    }

    static class TaskExecutor implements ClusterStateTaskExecutor<Integer> {
        List<Integer> tasks = new ArrayList<>();

        @Override
        public ClusterTasksResult<Integer> execute(ClusterState currentState, List<Integer> tasks) throws Exception {
            this.tasks.addAll(tasks);
            return ClusterTasksResult.<Integer>builder().successes(tasks).build(currentState);
        }
    }

    // test that for a single thread, tasks are executed in the order
    // that they are submitted
    public void testClusterStateUpdateTasksAreExecutedInOrder() throws BrokenBarrierException, InterruptedException {


        int numberOfThreads = randomIntBetween(2, 8);
        TaskExecutor[] executors = new TaskExecutor[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            executors[i] = new TaskExecutor();
        }

        int tasksSubmittedPerThread = randomIntBetween(2, 1024);

        CopyOnWriteArrayList<Tuple<String, Throwable>> failures = new CopyOnWriteArrayList<>();
        CountDownLatch updateLatch = new CountDownLatch(numberOfThreads * tasksSubmittedPerThread);

        ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure: [{}]", source), e);
                failures.add(new Tuple<>(source, e));
                updateLatch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
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
                        clusterTaskExecutor.submitStateUpdateTask("[" + index + "][" + j + "]", j,
                            ClusterStateTaskConfig.build(randomFrom(Priority.values())), executors[index], listener);
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
        Map<Integer, ClusterStateTaskListener> tasks = new HashMap<>();
        final int numOfTasks = randomInt(10);
        final CountDownLatch latch = new CountDownLatch(numOfTasks);
        for (int i = 0; i < numOfTasks; i++) {
            while (null != tasks.put(randomInt(1024), new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail(ExceptionsHelper.detailedMessage(e));
                }
            })) ;
        }

        clusterTaskExecutor.submitStateUpdateTasks("test", tasks, ClusterStateTaskConfig.build(Priority.LANGUID),
            (currentState, taskList) -> {
                assertThat(taskList.size(), equalTo(tasks.size()));
                assertThat(taskList.stream().collect(Collectors.toSet()), equalTo(tasks.keySet()));
                return ClusterStateTaskExecutor.ClusterTasksResult.<Integer>builder().successes(taskList).build(currentState);
            });

        latch.await();
    }

    /**
     * Note, this test can only work as long as we have a single thread executor executing the state update tasks!
     */
    public void testPrioritizedTasks() throws Exception {
        BlockingTask block = new BlockingTask(Priority.IMMEDIATE);
        clusterTaskExecutor.submitStateUpdateTask("test", block);
        int taskCount = randomIntBetween(5, 20);

        // will hold all the tasks in the order in which they were executed
        List<PrioritizedTask> tasks = new ArrayList<>(taskCount);
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            Priority priority = randomFrom(Priority.values());
            clusterTaskExecutor.submitStateUpdateTask("test", new PrioritizedTask(priority, latch, tasks));
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

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", BaseFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        clusterTaskExecutor.submitStateUpdateTask(
            "testBlockingCallInClusterStateTaskListenerFails",
            new Object(),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            new ClusterStateTaskExecutor<Object>() {
                @Override
                public ClusterTasksResult<Object> execute(ClusterState currentState, List<Object> tasks) throws Exception {
                    ClusterState newClusterState = ClusterState.builder(currentState).build();
                    return ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                }
            },
            new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    BaseFuture<Void> future = new BaseFuture<Void>() {};
                    try {
                        if (randomBoolean()) {
                            future.get(1L, TimeUnit.SECONDS);
                        } else {
                            future.get();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } catch (AssertionError e) {
                        assertionRef.set(e);
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                }
            }
        );

        latch.await();
        assertNotNull(assertionRef.get());
        assertThat(assertionRef.get().getMessage(),
            containsString("Reason: [Blocking operation]"));
    }

    public void testDuplicateSubmission() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        try (BlockingTask blockingTask = new BlockingTask(Priority.IMMEDIATE)) {
            clusterTaskExecutor.submitStateUpdateTask("blocking", blockingTask);

            ClusterStateTaskExecutor<SimpleTask> executor = (currentState, tasks) ->
                ClusterStateTaskExecutor.ClusterTasksResult.<SimpleTask>builder().successes(tasks).build(currentState);

            SimpleTask task = new SimpleTask(1);
            ClusterStateTaskListener listener = new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail(ExceptionsHelper.detailedMessage(e));
                }
            };

            clusterTaskExecutor.submitStateUpdateTask("first time", task, ClusterStateTaskConfig.build(Priority.NORMAL), executor,
                listener);

            final IllegalStateException e =
                expectThrows(
                    IllegalStateException.class,
                    () -> clusterTaskExecutor.submitStateUpdateTask(
                        "second time",
                        task,
                        ClusterStateTaskConfig.build(Priority.NORMAL),
                        executor, listener));
            assertThat(e, hasToString(containsString("task [1] with source [second time] is already queued")));

            clusterTaskExecutor.submitStateUpdateTask("third time a charm", new SimpleTask(1),
                ClusterStateTaskConfig.build(Priority.NORMAL), executor, listener);

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

    private static class BlockingTask extends ClusterStateUpdateTask implements Releasable {
        private final CountDownLatch latch = new CountDownLatch(1);

        public BlockingTask(Priority priority) {
            super(priority);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            latch.await();
            return currentState;
        }

        @Override
        public void onFailure(String source, Exception e) {
        }

        public void close() {
            latch.countDown();
        }

    }

    private static class PrioritizedTask extends ClusterStateUpdateTask {

        private final CountDownLatch latch;
        private final List<PrioritizedTask> tasks;

        private PrioritizedTask(Priority priority, CountDownLatch latch, List<PrioritizedTask> tasks) {
            super(priority);
            this.latch = latch;
            this.tasks = tasks;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            tasks.add(this);
            latch.countDown();
            return currentState;
        }

        @Override
        public void onFailure(String source, Exception e) {
            latch.countDown();
        }
    }

}

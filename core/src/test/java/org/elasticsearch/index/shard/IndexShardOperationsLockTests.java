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

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexShardOperationsLockTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("IndexShardOperationsLockTests");
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testAllOperationsInvoked() throws InterruptedException, TimeoutException, ExecutionException {
        IndexShardOperationsLock block = new IndexShardOperationsLock(logger, threadPool);
        int numThreads = 10;

        List<PlainActionFuture<Releasable>> futures = new ArrayList<>();
        List<Thread> operationThreads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads / 2);
        for (int i = 0; i < numThreads; i++) {
            PlainActionFuture<Releasable> future = new PlainActionFuture<>();
            Thread thread = new Thread() {
                public void run() {
                    latch.countDown();
                    block.acquire(future, ThreadPool.Names.GENERIC);
                }
            };
            futures.add(future);
            operationThreads.add(thread);
        }

        AtomicBoolean successFullyBlocked = new AtomicBoolean();


        Thread blockingThread = new Thread() {
            public void run() {
                try {
                    latch.await();
                    block.blockOperations(1, TimeUnit.MINUTES, () -> successFullyBlocked.set(true));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        blockingThread.start();

        for (Thread thread : operationThreads) {
            thread.start();
        }

        for (PlainActionFuture<Releasable> future : futures) {
            Releasable releasable = future.get(1, TimeUnit.MINUTES);
            assertNotNull(releasable);
            releasable.close();
        }

        for (Thread thread : operationThreads) {
            thread.join();
        }

        blockingThread.join();

        assertTrue(successFullyBlocked.get());
    }

    public void testFailureToBlockTwice() throws InterruptedException {
        IndexShardOperationsLock block = new IndexShardOperationsLock(logger, threadPool);
        CountDownLatch blockAcquired = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);

        Thread blockingThread = new Thread() {
            public void run() {
                try {
                    block.blockOperations(1, TimeUnit.MINUTES, () -> {
                        try {
                            blockAcquired.countDown();
                            releaseBlock.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        blockingThread.start();
        blockAcquired.await();

        expectThrows(IllegalStateException.class, () -> block.blockOperations(1, TimeUnit.MINUTES, () -> {}));

        releaseBlock.countDown();
        blockingThread.join();
    }

    public void testOperationsInvokedImmediatelyIfNoBlock() throws ExecutionException, InterruptedException {
        IndexShardOperationsLock block = new IndexShardOperationsLock(logger, threadPool);

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        block.acquire(future, ThreadPool.Names.GENERIC);
        assertTrue(future.isDone());
        future.get().close();
    }

    public void testOperationsDelayedIfBlock() throws ExecutionException, InterruptedException, TimeoutException {
        IndexShardOperationsLock block = new IndexShardOperationsLock(logger, threadPool);
        CountDownLatch blockAcquired = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);

        Thread blockingThread = new Thread() {
            public void run() {
                try {
                    block.blockOperations(1, TimeUnit.MINUTES, () -> {
                        try {
                            blockAcquired.countDown();
                            releaseBlock.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        blockingThread.start();
        blockAcquired.await();

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        block.acquire(future, ThreadPool.Names.GENERIC);
        assertFalse(future.isDone());

        releaseBlock.countDown();

        future.get(1, TimeUnit.MINUTES).close();

        blockingThread.join();
    }

    public void testOperationsDelayedIfBlockFailure() throws ExecutionException, InterruptedException, TimeoutException {
        IndexShardOperationsLock block = new IndexShardOperationsLock(logger, threadPool);
        CountDownLatch blockAcquired = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);

        final IndexShardClosedException exception = new IndexShardClosedException(new ShardId("blubb", "id", 0));
        Thread blockingThread = new Thread() {
            public void run() {
                try {
                    block.blockOperations(1, TimeUnit.MINUTES, () -> {
                        try {
                            blockAcquired.countDown();
                            releaseBlock.await();
                            throw exception;
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    });
                } catch (Exception e) {
                    if (e != exception) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        blockingThread.start();
        blockAcquired.await();

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        block.acquire(future, ThreadPool.Names.GENERIC);
        assertFalse(future.isDone());

        releaseBlock.countDown();

        future.get(1, TimeUnit.MINUTES).close();

        blockingThread.join();
    }
}

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
package org.elasticsearch.discovery.zen2;

import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.ConsensusState.AcceptedState;
import org.elasticsearch.discovery.zen2.ConsensusStateTests.ClusterState;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.index.translog.TranslogTests;
import org.elasticsearch.index.translog.TranslogTests.FailSwitch;
import org.elasticsearch.index.translog.TranslogTests.ThrowingFileChannel;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.core.IsEqual.equalTo;

public class ConsensusStorageTests extends ESTestCase {

    public void testRollover() {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        ConsensusState.NodeCollection initialConfig = new ConsensusState.NodeCollection();
        initialConfig.add(node);
        ClusterState clusterState1 = new ClusterState(1L, initialConfig, 42);
        final Path path;
        final ClusterState clusterState3;
        try (ConsensusStorage<ClusterState> st = createFreshStore(Settings.builder().put(
            ConsensusStorage.CS_LOG_RETENTION_SIZE_SETTING.getKey(), "1b").build(), clusterState1)) {

            path = st.getPath();

            assertThat(st.getGeneration(), equalTo(0L));

            ClusterState clusterState2 = new ClusterState(2L, initialConfig, 43);
            st.setCommittedState(clusterState2);
            assertThat(st.getGeneration(), equalTo(1L));

            clusterState3 = new ClusterState(3L, initialConfig, 44);
            AcceptedState<ClusterState> acceptedState = new AcceptedState<>(3L, clusterState3.diff(clusterState2));
            st.setAcceptedState(acceptedState);
            assertThat(st.getGeneration(), equalTo(2L));

            st.setCurrentTerm(2L);
            assertThat(st.getGeneration(), equalTo(2L));

            st.markLastAcceptedStateAsCommitted();
            assertThat(st.getGeneration(), equalTo(3L));
        }

        try (ConsensusStorage<ClusterState> st = createExistingStore(Settings.EMPTY, path)) {
            assertEquals(st.getCurrentTerm(), 2L);
            assertEquals(st.getCommittedState(), clusterState3);
            assertEquals(st.getAcceptedState(), Optional.empty());
            assertThat(st.getGeneration(), equalTo(3L));
        }
    }

    public void testNoRollover() {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        ConsensusState.NodeCollection initialConfig = new ConsensusState.NodeCollection();
        initialConfig.add(node);
        ClusterState clusterState1 = new ClusterState(1L, initialConfig, 42);
        final Path path;
        final ClusterState clusterState3;
        try (ConsensusStorage<ClusterState> st = createFreshStore(Settings.EMPTY, clusterState1)) {

            path = st.getPath();

            assertThat(st.getGeneration(), equalTo(0L));

            ClusterState clusterState2 = new ClusterState(2L, initialConfig, 43);
            st.setCommittedState(clusterState2);
            assertThat(st.getGeneration(), equalTo(1L)); // we always rollover on full CS update

            clusterState3 = new ClusterState(3L, initialConfig, 44);
            AcceptedState<ClusterState> acceptedState = new AcceptedState<>(3L, clusterState3.diff(clusterState2));
            st.setAcceptedState(acceptedState);
            assertThat(st.getGeneration(), equalTo(1L));

            st.setCurrentTerm(2L);
            assertThat(st.getGeneration(), equalTo(1L));

            st.markLastAcceptedStateAsCommitted();
            assertThat(st.getGeneration(), equalTo(1L));
        }

        try (ConsensusStorage<ClusterState> st = createExistingStore(Settings.EMPTY, path)) {
            assertEquals(st.getCurrentTerm(), 2L);
            assertEquals(st.getCommittedState(), clusterState3);
            assertEquals(st.getAcceptedState(), Optional.empty());
            assertThat(st.getGeneration(), equalTo(1L));
        }

    }

    public void testRandomFailure() throws IOException {
        Path path = createTempDir();
        FailSwitch fail = new FailSwitch();

        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        ConsensusState.NodeCollection initialConfig = new ConsensusState.NodeCollection();
        initialConfig.add(node);
        ClusterState clusterState1 = new ClusterState(1L, initialConfig, 42);

        boolean partialWrites = randomBoolean();
        boolean throwUnknownExceptions = randomBoolean();

        ClusterState clusterState2 = new ClusterState(2L, initialConfig, 43);
        boolean hasFailure;

        try (ConsensusStorage st = getFailableConsensusStorage(Settings.EMPTY, path, fail, partialWrites, throwUnknownExceptions)) {

            st.createFreshStore(0L, clusterState1);

            assertEquals(st.getGeneration(), 0L);
            assertTrue(Files.exists(path.resolve(ConsensusStorage.logFileName(0L))));

            fail.failRandomly();

            hasFailure = catchExpectedFailures(st, throwUnknownExceptions, () -> {
                if (randomBoolean()) {
                    st.setCommittedState(clusterState2);
                    assertEquals(1L, st.getGeneration());
                    assertTrue(Files.exists(path.resolve(ConsensusStorage.logFileName(1L))));
                    assertFalse(Files.exists(path.resolve(ConsensusStorage.logFileName(0L))));
                    logger.info("successfully updated to cluster state 2");
                } else if (randomBoolean()) {
                    st.setAcceptedState(new AcceptedState(4L, clusterState2.diff(clusterState1)));
                    st.markLastAcceptedStateAsCommitted();
                    assertEquals(0L, st.getGeneration());
                    assertTrue(Files.exists(path.resolve(ConsensusStorage.logFileName(0L))));
                    assertFalse(Files.exists(path.resolve(ConsensusStorage.logFileName(1L))));
                    logger.info("successfully updated to cluster state 2");
                } else {
                    st.setCurrentTerm(4L);
                }
            });

            if (hasFailure) {
                // check that state is not changed
                assertEquals(0L, st.getCurrentTerm());
                assertEquals(clusterState1, st.getCommittedState());
            }
        }

        // try recovery with injected failures, to make sure that clean up actions only happen after successful recovery
        try (ConsensusStorage st = getFailableConsensusStorage(Settings.EMPTY, path, fail, partialWrites, throwUnknownExceptions)) {

            fail.failRandomly();

            catchExpectedFailures(st, throwUnknownExceptions,
                () -> st.recoverFromExistingStore(ClusterState::new, in -> ClusterState.<ClusterState>readDiffFrom(ClusterState::new, in)));
        }

        // try recovery without injecting failures
        AcceptedState<ClusterState> acceptedState;
        try (ConsensusStorage<ClusterState> st = createExistingStore(Settings.EMPTY, path)) {
            if (hasFailure) {
                assertThat(st.getCommittedState(), either(equalTo(clusterState1)).or(equalTo(clusterState2)));
                // check that orphaned files have been deleted upon recovery
                try (Stream<Path> fileList = Files.list(path)) {
                    assertEquals(1L, fileList.filter(file -> file.getFileName().toString().endsWith(ConsensusStorage.LOG_SUFFIX)).count());
                }
            } else {
                if (st.getCurrentTerm() != 4L) {
                    assertEquals(st.getCommittedState(), clusterState2);
                }
            }

            // check that we can write to recovered state
            ClusterState clusterState3 = new ClusterState(3L, initialConfig, 44);
            acceptedState = new AcceptedState<>(4L, clusterState3.diff(st.getCommittedState()));
            st.setAcceptedState(acceptedState);
        }

        try (ConsensusStorage<ClusterState> st = createExistingStore(Settings.EMPTY, path)) {
            assertEquals(Optional.of(acceptedState), st.getAcceptedState());
        }

    }

    private boolean catchExpectedFailures(ConsensusStorage<ClusterState> st, boolean throwUnknownExceptions, Runnable runnable) {
        try {
            runnable.run();
            return false;
        } catch (UncheckedIOException e) {
            logger.info("simulated exception", e);
            assertFalse(st.isOpen());
            try {
                throw e.getCause();
            } catch (MockDirectoryWrapper.FakeIOException ex) {
                // ok
            } catch (IOException ex) {
                assertEquals("__FAKE__ no space left on device", ex.getMessage());
            }
            return true;
        } catch (TranslogTests.UnknownException e) {
            logger.info("simulated exception", e);
            assertFalse(st.isOpen());
            assertTrue(throwUnknownExceptions);
            return true;
        }
    }

    private Checkpoint randomCheckpoint() {
        return new Checkpoint(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    public void testCheckpointOnDiskFull() throws IOException {
        final Checkpoint checkpoint = randomCheckpoint();
        Path tempDir = createTempDir();
        Checkpoint.write(FileChannel::open, tempDir.resolve("foo.cpk"), checkpoint, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE_NEW);
        final Checkpoint checkpoint2 = randomCheckpoint();
        try {
            Checkpoint.write((p, o) -> {
                if (randomBoolean()) {
                    throw new MockDirectoryWrapper.FakeIOException();
                }
                FileChannel open = FileChannel.open(p, o);
                FailSwitch failSwitch = new FailSwitch();
                failSwitch.failNever(); // don't fail in the ctor
                ThrowingFileChannel channel = new ThrowingFileChannel(failSwitch, false, false, open);
                failSwitch.failAlways();
                return channel;

            }, tempDir.resolve("foo.cpk"), checkpoint2, StandardOpenOption.WRITE);
            fail("should have failed earlier");
        } catch (MockDirectoryWrapper.FakeIOException ex) {
            //fine
        }
        Checkpoint read = Checkpoint.read(tempDir.resolve("foo.cpk"));
        assertEquals(read, checkpoint);
    }

    public static ConsensusStorage<ClusterState> getFailableConsensusStorage(Settings settings, Path path, FailSwitch fail,
                                                                             boolean partialWrites, boolean throwUnknownException)
        throws IOException {
        return new ConsensusStorage<ClusterState>(settings, path) {
            @Override
            ChannelFactory getChannelFactory() {
                final ChannelFactory factory = super.getChannelFactory();

                return (file, openOption) -> {
                    FileChannel channel = factory.open(file, openOption);
                    boolean success = false;
                    try {
                        // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                        final boolean isCkpFile = file.getFileName().toString().endsWith(ConsensusStorage.CHECKPOINT_SUFFIX);
                        ThrowingFileChannel throwingFileChannel = new ThrowingFileChannel(fail, isCkpFile ? false : partialWrites,
                            throwUnknownException, channel);
                        success = true;
                        return throwingFileChannel;
                    } finally {
                        if (success == false) {
                            IOUtils.closeWhileHandlingException(channel);
                        }
                    }
                };
            }
        };
    }

    public static ConsensusStorage<ClusterState> createFreshStore(Settings settings, ClusterState initialClusterState) {
        final ConsensusStorage storage = new ConsensusStorage(settings, createTempDir());
        storage.createFreshStore(0L, initialClusterState);
        return storage;
    }

    public static ConsensusStorage<ClusterState> createExistingStore(Settings settings, Path path) {
        final ConsensusStorage storage = new ConsensusStorage(settings, path);
        storage.recoverFromExistingStore(ClusterState::new, in -> ClusterState.<ClusterState>readDiffFrom(ClusterState::new, in));
        return storage;
    }

}

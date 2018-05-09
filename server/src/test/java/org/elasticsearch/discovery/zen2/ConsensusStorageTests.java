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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
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
import java.util.Collections;
import java.util.stream.Stream;

import static org.elasticsearch.discovery.zen2.ConsensusStateTests.value;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.core.IsEqual.equalTo;

public class ConsensusStorageTests extends ESTestCase {

    public void testRollover() {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node.getId()));
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));

        ClusterState clusterState1 = ConsensusStateTests.clusterState(1L, 1L, node, initialConfig, initialConfig, 42);
        final Path path;
        final ClusterState clusterState3;
        try (ConsensusStorage st = createFreshStore(Settings.builder().put(
            ConsensusStorage.CS_LOG_RETENTION_SIZE_SETTING.getKey(), "1b").build(), clusterState1, node)) {

            path = st.getPath();

            assertThat(st.getGeneration(), equalTo(0L));

            ClusterState clusterState2 = ConsensusStateTests.nextStateWithValue(clusterState1, 43);
            st.setLastAcceptedState(clusterState2);
            assertThat(st.getGeneration(), equalTo(1L));

            clusterState3 = ConsensusStateTests.nextStateWithTermValueAndConfig(clusterState2, 1L, 44, newConfig);
            st.setLastAcceptedState(clusterState3);
            assertThat(st.getGeneration(), equalTo(2L));

            st.setCurrentTerm(2L);
            assertThat(st.getGeneration(), equalTo(2L));

            st.markLastAcceptedConfigAsCommitted();
            assertThat(st.getGeneration(), equalTo(3L));
        }

        try (ConsensusStorage st = createExistingStore(Settings.EMPTY, path, node)) {
            assertEquals(st.getCurrentTerm(), 2L);
            assertEquals(value(st.getLastAcceptedState()), 44L);
            assertEquals(st.getLastAcceptedState().term(), 1L);
            assertEquals(st.getLastAcceptedState().version(), 3L);
            assertEquals(st.getLastAcceptedState().getLastAcceptedConfiguration(), newConfig);
            assertEquals(st.getLastAcceptedState().getLastCommittedConfiguration(), newConfig);
            assertThat(st.getGeneration(), equalTo(3L));
        }
    }

    public void testNoRollover() {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node.getId()));
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));

        ClusterState clusterState1 = ConsensusStateTests.clusterState(1L, 1L, node, initialConfig, initialConfig, 42);
        final Path path;
        final ClusterState clusterState3;
        try (ConsensusStorage st = createFreshStore(Settings.EMPTY, clusterState1, node)) {

            path = st.getPath();

            assertThat(st.getGeneration(), equalTo(0L));

            ClusterState clusterState2 = ConsensusStateTests.nextStateWithValue(clusterState1, 43);
            st.setLastAcceptedState(clusterState2);
            assertThat(st.getGeneration(), equalTo(0L));

            clusterState3 = ConsensusStateTests.nextStateWithTermValueAndConfig(clusterState2, 1L, 44, newConfig);
            st.setLastAcceptedState(clusterState3);
            assertThat(st.getGeneration(), equalTo(0L));

            st.setCurrentTerm(2L);
            assertThat(st.getGeneration(), equalTo(0L));

            st.markLastAcceptedConfigAsCommitted();
            assertThat(st.getGeneration(), equalTo(0L));
        }

        try (ConsensusStorage st = createExistingStore(Settings.EMPTY, path, node)) {
            assertEquals(st.getCurrentTerm(), 2L);
            assertEquals(value(st.getLastAcceptedState()), 44L);
            assertEquals(st.getLastAcceptedState().term(), 1L);
            assertEquals(st.getLastAcceptedState().version(), 3L);
            assertEquals(st.getLastAcceptedState().getLastAcceptedConfiguration(), newConfig);
            assertEquals(st.getLastAcceptedState().getLastCommittedConfiguration(), newConfig);
            assertThat(st.getGeneration(), equalTo(0L));
        }

    }

    public void testRandomFailure() throws IOException {
        Path path = createTempDir();
        FailSwitch fail = new FailSwitch();

        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration initialConfig = new VotingConfiguration(Collections.singleton(node.getId()));

        ClusterState clusterState1 = ConsensusStateTests.clusterState(1L, 1L, node, initialConfig, initialConfig, 42);

        boolean rollOver = randomBoolean();
        Settings storageSettings = rollOver ? Settings.builder().put(
            ConsensusStorage.CS_LOG_RETENTION_SIZE_SETTING.getKey(), "1b").build() : Settings.EMPTY;
        boolean partialWrites = randomBoolean();
        boolean throwUnknownExceptions = randomBoolean();

        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        VotingConfiguration newConfig = new VotingConfiguration(Collections.singleton(node2.getId()));

        ClusterState clusterState2 = ConsensusStateTests.nextStateWithTermValueAndConfig(clusterState1, 1L, 43, newConfig);
        boolean hasFailure;
        boolean committed = false;

        try (ConsensusStorage st = getFailableConsensusStorage(storageSettings, path, node, fail, partialWrites, throwUnknownExceptions)) {

            st.createFreshStore(1L, clusterState1);

            assertEquals(st.getGeneration(), 0L);
            assertTrue(Files.exists(path.resolve(ConsensusStorage.logFileName(0L))));

            fail.failRandomly();

            hasFailure = catchExpectedFailures(st, throwUnknownExceptions, () -> {
                if (rollOver) {
                    st.setLastAcceptedState(clusterState2);
                    assertEquals(1L, st.getGeneration());
                    assertTrue(Files.exists(path.resolve(ConsensusStorage.logFileName(1L))));
                    assertFalse(Files.exists(path.resolve(ConsensusStorage.logFileName(0L))));
                    logger.info("successfully updated to cluster state 2");
                } else if (randomBoolean()) {
                    st.setLastAcceptedState(clusterState2);
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
                assertEquals(1L, st.getCurrentTerm());
                assertEquals(value(clusterState1), value(st.getLastAcceptedState()));
            } else {
                if (value(clusterState2) == value(st.getLastAcceptedState())) {
                    committed = !catchExpectedFailures(st, throwUnknownExceptions, () -> {
                        st.markLastAcceptedConfigAsCommitted();
                        logger.info("successfully committed cluster state 2");
                    });
                }
            }
        }

        // try recovery with injected failures, to make sure that clean up actions only happen after successful recovery
        try (ConsensusStorage st = getFailableConsensusStorage(Settings.EMPTY, path, node, fail, partialWrites, throwUnknownExceptions)) {

            fail.failRandomly();

            NamedWriteableRegistry registry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());

            catchExpectedFailures(st, throwUnknownExceptions, () -> st.recoverFromExistingStore(registry));
        }

        // try recovery without injecting failures
        try (ConsensusStorage st = createExistingStore(Settings.EMPTY, path, node)) {
            if (hasFailure) {
                assertThat(value(st.getLastAcceptedState()), either(equalTo(value(clusterState1))).or(equalTo(value(clusterState2))));
                assertThat(st.getLastAcceptedState().getLastCommittedConfiguration(), equalTo(initialConfig));
                // check that orphaned files have been deleted upon recovery
                try (Stream<Path> fileList = Files.list(path)) {
                    assertEquals(1L, fileList.filter(file -> file.getFileName().toString().endsWith(ConsensusStorage.LOG_SUFFIX)).count());
                }
            } else {
                if (st.getCurrentTerm() != 4L) {
                    assertEquals(value(st.getLastAcceptedState()), value(clusterState2));
                }
                if (committed) {
                    assertThat(st.getLastAcceptedState().getLastCommittedConfiguration(), equalTo(newConfig));
                }
            }

            // check that we can write to recovered state
            ClusterState clusterState3 = ConsensusStateTests.nextStateWithValue(clusterState2, 44);
            st.setLastAcceptedState(clusterState3);
        }

        try (ConsensusStorage st = createExistingStore(Settings.EMPTY, path, node)) {
            assertEquals(value(st.getLastAcceptedState()), 44L);
        }

    }

    private boolean catchExpectedFailures(ConsensusStorage st, boolean throwUnknownExceptions, Runnable runnable) {
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

    public static ConsensusStorage getFailableConsensusStorage(Settings settings, Path path, DiscoveryNode localNode, FailSwitch fail,
                                                               boolean partialWrites, boolean throwUnknownException)
        throws IOException {
        return new ConsensusStorage(settings, path, localNode) {
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

    public static ConsensusStorage createFreshStore(Settings settings, ClusterState initialClusterState, DiscoveryNode localNode) {
        final ConsensusStorage storage = new ConsensusStorage(settings, createTempDir(), localNode);
        storage.createFreshStore(1L, initialClusterState);
        return storage;
    }

    public static ConsensusStorage createExistingStore(Settings settings, Path path, DiscoveryNode localNode) {
        final ConsensusStorage storage = new ConsensusStorage(settings, path, localNode);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        storage.recoverFromExistingStore(registry);
        return storage;
    }

}

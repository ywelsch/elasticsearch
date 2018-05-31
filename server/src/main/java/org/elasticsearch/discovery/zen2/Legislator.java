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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.NodeJoinController;
import org.elasticsearch.discovery.zen.NodeJoinController.JoinTaskExecutor;
import org.elasticsearch.discovery.zen.ZenDiscovery.NodeRemovalClusterStateTaskExecutor;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.AbdicationRequest;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckRequest;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PrejoinHandoverRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * A Legislator, following the Paxos metaphor, is responsible for making sure that actions happen in a timely fashion
 * and for turning messages from the outside world into actions performed on the ConsensusState.
 */
public class Legislator extends AbstractComponent {
    // TODO On the happy path (publish-and-commit) we log at TRACE and everything else is logged at DEBUG. Increase levels as appropriate.

    public static final Setting<TimeValue> CONSENSUS_MIN_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.min_delay",
            TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CONSENSUS_MAX_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.max_delay",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1000), Setting.Property.NodeScope);
    // the time in follower state without receiving new values or heartbeats from a leader before becoming a candidate again
    public static final Setting<Integer> CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING =
        Setting.intSetting("discovery.zen2.leader_check_retry_count", 3, 1, Setting.Property.NodeScope);
    // the time between heartbeats sent by the leader
    public static final Setting<TimeValue> CONSENSUS_HEARTBEAT_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.heartbeat_delay",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1000), Setting.Property.NodeScope);
    // the timeout for collecting a quorum of responses from a single heartbeat before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_HEARTBEAT_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.heartbeat_timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // the timeout for the publication of each value
    public static final Setting<TimeValue> CONSENSUS_PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.publication_timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final ConsensusState consensusState;
    private final Transport transport;
    private final MasterService masterService;
    private final DiscoveryNode localNode;
    private final Supplier<List<DiscoveryNode>> nodeSupplier;
    private final TimeValue minDelay;
    private final TimeValue maxDelay;
    private final TimeValue heartbeatDelay;
    private final TimeValue heartbeatTimeout;
    private final TimeValue publishTimeout;
    private final int leaderCheckRetryCount;
    private final FutureExecutor futureExecutor;
    private final LongSupplier currentTimeSupplier;
    private final Random random;
    private final ClusterApplier clusterApplier;
    private final ClusterBlock noMasterBlock; // TODO make dynamic
    private final LeaderCheckRequest leaderCheckRequest;

    private final Object mutex = new Object();

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private Optional<SeekJoinsScheduler> seekJoinsScheduler;
    private Optional<ActiveLeaderFailureDetector> activeLeaderFailureDetector;
    private Optional<ActiveFollowerFailureDetector> activeFollowerFailureDetector;
    private volatile LeaderCheckResponder leaderCheckResponder;
    private volatile HeartbeatRequestResponder heartbeatRequestResponder;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect join offers.
    private Optional<OfferJoinCollector> currentOfferJoinCollector;

    private volatile Optional<ClusterState> lastCommittedState; // the state that was last committed, exposed to the cluster state applier

    // similar to NodeJoinController.ElectionContext.joinRequestAccumulator, captures joins on election
    private final Map<DiscoveryNode, MembershipAction.JoinCallback> joinRequestAccumulator = new HashMap<>();

    private final JoinTaskExecutor joinTaskExecutor;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;

    private Optional<Publication> currentPublication = Optional.empty();
    private long laggingUntilCommittedVersionExceeds;

    public Legislator(Settings settings, ConsensusState.PersistedState persistedState,
                      Transport transport, MasterService masterService, AllocationService allocationService,
                      DiscoveryNode localNode, LongSupplier currentTimeSupplier,
                      FutureExecutor futureExecutor, Supplier<List<DiscoveryNode>> nodeSupplier,
                      ClusterApplier clusterApplier, Random random) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        leaderCheckRetryCount = CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING.get(settings);
        heartbeatDelay = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(settings);
        heartbeatTimeout = CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(settings);
        publishTimeout = CONSENSUS_PUBLISH_TIMEOUT_SETTING.get(settings);
        this.random = random;

        consensusState = new ConsensusState(settings, localNode, persistedState);
        this.transport = transport;
        this.masterService = masterService;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.futureExecutor = futureExecutor;
        this.nodeSupplier = nodeSupplier;
        this.clusterApplier = clusterApplier;
        this.leaderCheckRequest = new LeaderCheckRequest(localNode);
        lastKnownLeader = Optional.empty();
        lastCommittedState = Optional.empty();
        lastJoin = Optional.empty();
        currentOfferJoinCollector = Optional.empty();
        seekJoinsScheduler = Optional.empty();
        activeLeaderFailureDetector = Optional.empty();
        activeFollowerFailureDetector = Optional.empty();

        // disable minimum_master_nodes check
        final ElectMasterService electMasterService = new ElectMasterService(settings) {

            @Override
            public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
                return true;
            }

            @Override
            public void logMinimumMasterNodesWarningIfNecessary(ClusterState oldState, ClusterState newState) {
                // ignore
            }

        };

        // reuse JoinTaskExecutor implementation from ZenDiscovery, but hack some checks
        this.joinTaskExecutor = new JoinTaskExecutor(allocationService, electMasterService, logger) {

            @Override
            public ClusterTasksResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm;
                synchronized (mutex) {
                    currentTerm = consensusState.getCurrentTerm(); // TODO perhaps this can be a volatile read?
                }
                if (currentState.term() != currentTerm) {
                    // ZenDiscovery assumes that when a node becomes a fresh leader, that the previous cluster state had master node id set
                    // to null. As we're not doing such thing on becoming candidate (yet), we fake this here.
                    currentState = ClusterState.builder(currentState).term(currentTerm)
                        .nodes(DiscoveryNodes.builder(currentState.nodes()).masterNodeId(null)).build();
                }
                return super.execute(currentState, joiningNodes);
            }

        };

        // reuse NodeRemovalClusterStateTaskExecutor implementation from ZenDiscovery, but don't provide rejoin callback as
        // minimum_master_nodes check is disabled in the adapted ElectMasterService from above
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService,
            electMasterService,
            s -> {
                throw new AssertionError("not implemented");
            },
            logger);

        noMasterBlock = DiscoverySettings.NO_MASTER_BLOCK_SETTING.get(settings);

        assert localNode.equals(persistedState.getLastAcceptedState().nodes().getLocalNode()) :
            "local node mismatch, expected " + localNode + " but got " + persistedState.getLastAcceptedState().nodes().getLocalNode();
    }

    public void start() {
        synchronized (mutex) {
            // copied from ZenDiscovery#doStart()
            ClusterState.Builder builder = clusterApplier.newClusterStateBuilder();
            ClusterState initialState = builder
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(noMasterBlock))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
                .build();
            clusterApplier.setInitialState(initialState);
            // copied from ZenDiscovery#doStart()

            // TODO perhaps the code above needs no synchronisation?

            becomeCandidate("start");
        }
    }

    // only for testing
    Mode getMode() {
        synchronized (mutex) {
            return mode;
        }
    }

    public DiscoveryNode getLocalNode() {
        return localNode; // final and immutable so no lock needed
    }

    // only for testing
    void handleFailure() {
        synchronized (mutex) {
            if (mode == Mode.CANDIDATE) {
                logger.debug("handleFailure: already a candidate");
            } else {
                becomeCandidate("handleFailure");
            }
        }
    }

    private void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;

            clearJoins();
            assert seekJoinsScheduler.isPresent() == false;
            seekJoinsScheduler = Optional.of(new SeekJoinsScheduler());

            stopActiveLeaderFailureDetector();
            stopActiveFollowerFailureDetector();
            cancelActivePublication();

            leaderCheckResponder = new LeaderCheckResponder();
            heartbeatRequestResponder = new HeartbeatRequestResponder();
        }
    }

    private void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        assert mode != Mode.LEADER;

        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;

        assert activeLeaderFailureDetector.isPresent() == false;
        final ActiveLeaderFailureDetector activeLeaderFailureDetector = new ActiveLeaderFailureDetector();
        this.activeLeaderFailureDetector = Optional.of(activeLeaderFailureDetector);
        stopSeekJoinsScheduler();
        stopActiveFollowerFailureDetector();
        lastKnownLeader = Optional.of(localNode);
        leaderCheckResponder = new LeaderCheckResponder();
        heartbeatRequestResponder = new HeartbeatRequestResponder();
    }

    private void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";

        if (mode != Mode.FOLLOWER) {
            logger.debug("{}: becoming FOLLOWER of [{}] (was {}, lastKnownLeader was [{}])", method, leaderNode, mode, lastKnownLeader);

            assert activeFollowerFailureDetector.isPresent() == false;
            mode = Mode.FOLLOWER;
            final ActiveFollowerFailureDetector activeFollowerFailureDetector = new ActiveFollowerFailureDetector();
            this.activeFollowerFailureDetector = Optional.of(activeFollowerFailureDetector);
            activeFollowerFailureDetector.start();
        }

        lastKnownLeader = Optional.of(leaderNode);
        stopSeekJoinsScheduler();
        stopActiveLeaderFailureDetector();
        cancelActivePublication();
        leaderCheckResponder = new LeaderCheckResponder();
        heartbeatRequestResponder = new HeartbeatRequestResponder();
    }

    private void stopSeekJoinsScheduler() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (seekJoinsScheduler.isPresent()) {
            seekJoinsScheduler.get().stop();
            seekJoinsScheduler = Optional.empty();
        }
    }

    private void stopActiveLeaderFailureDetector() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (activeLeaderFailureDetector.isPresent()) {
            activeLeaderFailureDetector.get().stop();
            activeLeaderFailureDetector = Optional.empty();
        }
    }

    private void stopActiveFollowerFailureDetector() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (activeFollowerFailureDetector.isPresent()) {
            activeFollowerFailureDetector.get().stop();
            activeFollowerFailureDetector = Optional.empty();
        }
    }

    private void cancelActivePublication() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (currentPublication.isPresent()) {
            currentPublication.get().failActiveTargets();
            assert currentPublication.isPresent() == false;
        }
    }

    private void clearJoins() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        joinRequestAccumulator.values().forEach(
            joinCallback -> joinCallback.onFailure(new ConsensusMessageRejectedException("node stepped down as leader")));
        joinRequestAccumulator.clear();
    }

    private void startSeekingJoins() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        clearJoins(); // TODO: is this the right place?

        currentOfferJoinCollector = Optional.of(new OfferJoinCollector());
        SeekJoins seekJoins = new SeekJoins(localNode, consensusState.getCurrentTerm(), consensusState.getLastAcceptedVersion());
        currentOfferJoinCollector.get().start(seekJoins);
    }

    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("joinLeaderInTerm: from [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());
        Join join = consensusState.handleStartJoin(startJoinRequest);
        lastJoin = Optional.of(join);
        if (mode == Mode.CANDIDATE) {
            // refresh required because current term has changed
            heartbeatRequestResponder = new HeartbeatRequestResponder();
        } else {
            // becomeCandidate refreshes responders
            becomeCandidate("joinLeaderInTerm");
        }
        return join;
    }

    public void handleStartJoin(StartJoinRequest startJoinRequest) {
        synchronized (mutex) {
            sendJoin(startJoinRequest.getSourceNode(), joinLeaderInTerm(startJoinRequest));
        }
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (consensusState.getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    /**
     * Publish a new cluster state. May throw ConsensusMessageRejectedException (NB, not FailedToCommitClusterStateException) if the
     * publication cannot even be started, or else notifies the given completionListener on completion.
     *
     * @param clusterState       The new cluster state to publish.
     * @param completionListener Receives notification of completion of the publication, whether successful, failed, or timed out.
     * @param ackListener        Receives notification of success or failure for each individual node, but not if timed out.
     */
    public void handleClientValue(ClusterState clusterState, ActionListener<Void> completionListener, AckListener ackListener) {
        synchronized (mutex) {
            handleClientValueUnderLock(clusterState, completionListener, ackListener);
        }
    }

    private void handleClientValueUnderLock(ClusterState clusterState, ActionListener<Void> completionListener, AckListener ackListener) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";

        if (mode != Mode.LEADER) {
            throw new ConsensusMessageRejectedException("handleClientValue: not currently leading, so cannot handle client value.");
        }

        if (currentPublication.isPresent()) {
            throw new ConsensusMessageRejectedException("[{}] is in progress", currentPublication.get());
        }

        assert localNode.equals(clusterState.getNodes().get(localNode.getId())) : localNode + " should be in published " + clusterState;

        final PublishRequest publishRequest = consensusState.handleClientValue(clusterState);
        final Publication publication = new Publication(publishRequest, completionListener, ackListener);

        assert currentPublication.isPresent() == false
            : "[" + currentPublication.get() + "] in progress, cannot start [" + publication + ']';
        currentPublication = Optional.of(publication);

        futureExecutor.schedule(publishTimeout, "Publication#onTimeout", publication::onTimeout);
        activeLeaderFailureDetector.get().updateNodesAndPing(publishRequest.getAcceptedState());
        publication.start();
        // TODO when we have a notion of "completion" of a publication, if a publication completes and there are faulty nodes then
        // we should try and remove them. TBD maybe this duplicates the queueing of tasks that occurs in the MasterService?
    }

    public LeaderCheckResponse handleLeaderCheckRequest(LeaderCheckRequest leaderCheckRequest) {
        return leaderCheckResponder.handleLeaderCheckRequest(leaderCheckRequest);
    }

    // only for testing
    ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return consensusState.getLastAcceptedState();
        }
    }

    // only for testing
    long getCurrentTerm() {
        synchronized (mutex) {
            return consensusState.getCurrentTerm();
        }
    }

    public Optional<ClusterState> getLastCommittedState() {
        return lastCommittedState; // volatile read, needs no mutex
    }

    public ClusterState getStateForMasterService() {
        synchronized (mutex) {
            //TODO: set masterNodeId to null in cluster state when we're not leader
            return consensusState.getLastAcceptedState();
        }
    }

    public enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        WAITING_FOR_QUORUM,
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
    }

    private class Publication {

        private final AtomicReference<ApplyCommit> applyCommitReference;
        private final List<PublicationTarget> publicationTargets;
        private final PublishRequest publishRequest;
        private final ActionListener<Void> completionListener;
        private final AckListener ackListener;
        private boolean isCompleted;

        private Publication(PublishRequest publishRequest, ActionListener<Void> completionListener, AckListener ackListener) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            this.publishRequest = publishRequest;
            this.completionListener = completionListener;
            this.ackListener = ackListener;
            applyCommitReference = new AtomicReference<>();

            publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
            publishRequest.getAcceptedState().getNodes().iterator().forEachRemaining(n -> publicationTargets.add(new PublicationTarget(n)));
        }

        @Override
        public String toString() {
            // everything here is immutable so no synchronisation required
            return "Publication{term=" + publishRequest.getAcceptedState().term() +
                ", version=" + publishRequest.getAcceptedState().version() + '}';
        }

        private void start() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            logger.trace("publishing {} to {}", publishRequest, publicationTargets);

            Set<DiscoveryNode> localFaultyNodes = new HashSet<>(activeLeaderFailureDetector.get().faultyNodes);
            for (final DiscoveryNode faultyNode : localFaultyNodes) {
                onFaultyNode(faultyNode);
            }
            publicationTargets.forEach(PublicationTarget::sendPublishRequest);
            onPossibleCommitFailure();
        }

        private void onCompletion() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert isCompleted == false;
            isCompleted = true;
            assert currentPublication.get() == this;
            currentPublication = Optional.empty();
        }

        private void onPossibleCompletion() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            if (isCompleted) {
                assert currentPublication.isPresent() == false || currentPublication.get() != this;
                return;
            }

            assert currentPublication.get() == this : "completing [" + this + "], currentPublication = [" + currentPublication.get() + ']';

            for (final PublicationTarget target : publicationTargets) {
                if (target.state.isActive()) {
                    return;
                }
            }

            for (final PublicationTarget target : publicationTargets) {
                if (target.discoveryNode.equals(localNode) && target.state.isFailed()) {
                    logger.debug("onPossibleCompletion: [{}] failed on master", this);
                    onCompletion();
                    FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException("publish-to-self failed");
                    ackListener.onNodeAck(localNode, exception); // other nodes have acked, but not the master.
                    completionListener.onFailure(exception);
                    return;
                }
            }

            assert consensusState.getLastAcceptedTerm() == applyCommitReference.get().term
                && consensusState.getLastAcceptedVersion() == applyCommitReference.get().version
                : "onPossibleCompletion: term or version mismatch when publishing [" + this
                + "]: current version is now [" + consensusState.getLastAcceptedVersion()
                + "] in term [" + consensusState.getLastAcceptedTerm() + "]";

            onCompletion();
            assert applyCommitReference.get() != null;
            logger.trace("onPossibleCompletion: [{}] was successful, applying new state locally", this);
            clusterApplier.onNewClusterState(this.toString(), () -> getLastCommittedState().get(), new ClusterApplyListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    synchronized (mutex) {
                        becomeCandidate("clusterApplier#onNewClusterState");
                    }
                    ackListener.onNodeAck(localNode, e);
                    completionListener.onFailure(e);
                }

                @Override
                public void onSuccess(String source) {
                    ackListener.onNodeAck(localNode, null);
                    completionListener.onResponse(null);
                }
            });
        }

        // For assertions only: verify that this invariant holds
        private boolean publicationCompletedIffAllTargetsInactive() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            for (final PublicationTarget target : publicationTargets) {
                if (target.state.isActive()) {
                    return isCompleted == false;
                }
            }
            return isCompleted;
        }

        private void onCommitted(final ApplyCommit applyCommit) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert applyCommitReference.get() == null;
            applyCommitReference.set(applyCommit);
            publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum).forEach(PublicationTarget::sendApplyCommit);
        }

        private void onPossibleCommitFailure() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            if (applyCommitReference.get() != null) {
                onPossibleCompletion();
                return;
            }

            NodeCollection possiblySuccessfulNodes = new NodeCollection();
            for (PublicationTarget publicationTarget : publicationTargets) {
                if (publicationTarget.state.mayCommitInFuture()) {
                    possiblySuccessfulNodes.add(publicationTarget.discoveryNode);
                } else {
                    assert publicationTarget.state.isFailed() : publicationTarget.state;
                }
            }

            if (consensusState.electionWon() == false) {
                logger.debug("onPossibleCommitFailure: node stepped down as leader while publishing");
                failActiveTargets();
            }

            if (false == consensusState.isPublishQuorum(possiblySuccessfulNodes)) {
                logger.debug("onPossibleCommitFailure: non-failed nodes do not form a quorum, so publication cannot succeed");
                failActiveTargets();
                if (publishRequest.getAcceptedState().term() == consensusState.getCurrentTerm() &&
                    publishRequest.getAcceptedState().version() == consensusState.getLastPublishedVersion()) {
                    becomeCandidate("Publication.onPossibleCommitFailure");
                }
            }
        }

        void failActiveTargets() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(PublicationTarget::setFailed);
            onPossibleCompletion();
        }

        public void onTimeout() {
            synchronized (mutex) {
                publicationTargets.stream().filter(PublicationTarget::isActive).forEach(PublicationTarget::onTimeOut);
                onPossibleCompletion();

                if (mode == Mode.LEADER && applyCommitReference.get() == null) {
                    logger.debug("Publication.onTimeout(): failed to commit version [{}] in term [{}]",
                        publishRequest.getAcceptedState().version(), publishRequest.getAcceptedState().term());
                    becomeCandidate("Publication.onTimeout()");
                }
            }
        }

        void onFaultyNode(DiscoveryNode faultyNode) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            publicationTargets.forEach(t -> t.onFaultyNode(faultyNode));
            onPossibleCompletion();
        }

        private class PublicationTarget {
            private final DiscoveryNode discoveryNode;
            private final PublicationTargetStateMachine state = new PublicationTargetStateMachine();
            private boolean ackIsPending = true;

            private PublicationTarget(DiscoveryNode discoveryNode) {
                this.discoveryNode = discoveryNode;
            }

            @Override
            public String toString() {
                // everything here is immutable so no synchronisation required
                return discoveryNode.getId();
            }

            public void sendPublishRequest() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                if (state.isFailed()) {
                    return;
                }
                state.setState(PublicationTargetState.SENT_PUBLISH_REQUEST);
                transport.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
                // TODO Can this ^ fail with an exception? Target should be failed if so.
                assert publicationCompletedIffAllTargetsInactive();
            }

            void handlePublishResponse(PublishResponse publishResponse) {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                assert state.isWaitingForQuorum() : state;

                logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);
                assert consensusState.getCurrentTerm() >= publishResponse.getTerm();
                if (applyCommitReference.get() != null) {
                    sendApplyCommit();
                } else {
                    Optional<ApplyCommit> optionalCommit = consensusState.handlePublishResponse(discoveryNode, publishResponse);
                    optionalCommit.ifPresent(Publication.this::onCommitted);
                }
            }

            public void sendApplyCommit() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                state.setState(PublicationTargetState.SENT_APPLY_COMMIT);

                ApplyCommit applyCommit = applyCommitReference.get();
                assert applyCommit != null;

                transport.sendApplyCommit(discoveryNode, applyCommit, new ApplyCommitResponseHandler());
                assert publicationCompletedIffAllTargetsInactive();
            }

            public boolean isWaitingForQuorum() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                return state.isWaitingForQuorum();
            }

            public boolean isActive() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                return state.isActive();
            }

            public void setFailed() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                assert isActive();
                state.setState(PublicationTargetState.FAILED);
                ackOnce(new ElasticsearchException("publication failed"));
            }

            public void onTimeOut() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                assert isActive();
                state.setState(PublicationTargetState.FAILED);
                if (applyCommitReference.get() == null) {
                    ackOnce(new ElasticsearchException("publication timed out"));
                }
            }

            public void onFaultyNode(DiscoveryNode faultyNode) {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                if (isActive() && discoveryNode.equals(faultyNode)) {
                    logger.debug("onFaultyNode: [{}] is faulty, failing target in publication of version [{}] in term [{}]", faultyNode,
                        publishRequest.getAcceptedState().version(), publishRequest.getAcceptedState().term());
                    setFailed();
                    onPossibleCommitFailure();
                }
            }

            private void ackOnce(Exception e) {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                if (ackIsPending && localNode.equals(discoveryNode) == false) {
                    ackIsPending = false;
                    ackListener.onNodeAck(discoveryNode, e);
                }
            }

            private class PublicationTargetStateMachine {
                private PublicationTargetState state = PublicationTargetState.NOT_STARTED;

                public void setState(PublicationTargetState newState) {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    switch (newState) {
                        case NOT_STARTED:
                            assert false : state + " -> " + newState;
                            break;
                        case SENT_PUBLISH_REQUEST:
                            assert state == PublicationTargetState.NOT_STARTED : state + " -> " + newState;
                            break;
                        case WAITING_FOR_QUORUM:
                            assert state == PublicationTargetState.SENT_PUBLISH_REQUEST : state + " -> " + newState;
                            break;
                        case SENT_APPLY_COMMIT:
                            assert state == PublicationTargetState.WAITING_FOR_QUORUM : state + " -> " + newState;
                            break;
                        case APPLIED_COMMIT:
                            assert state == PublicationTargetState.SENT_APPLY_COMMIT : state + " -> " + newState;
                            break;
                        case FAILED:
                            assert state != PublicationTargetState.APPLIED_COMMIT : state + " -> " + newState;
                            break;
                    }
                    state = newState;
                }

                public boolean isActive() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    return state != PublicationTargetState.FAILED
                        && state != PublicationTargetState.APPLIED_COMMIT;
                }

                public boolean isWaitingForQuorum() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    return state == PublicationTargetState.WAITING_FOR_QUORUM;
                }

                public boolean mayCommitInFuture() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    return (state == PublicationTargetState.NOT_STARTED
                        || state == PublicationTargetState.SENT_PUBLISH_REQUEST
                        || state == PublicationTargetState.WAITING_FOR_QUORUM);
                }

                public boolean isFailed() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    return state == PublicationTargetState.FAILED;
                }

                @Override
                public String toString() {
                    // TODO DANGER non-volatile, mutable variable requires synchronisation
                    return state.toString();
                }
            }

            private class PublishResponseHandler implements TransportResponseHandler<LegislatorPublishResponse> {
                @Override
                public LegislatorPublishResponse read(StreamInput in) throws IOException {
                    return new LegislatorPublishResponse(in);
                }

                @Override
                public void handleResponse(LegislatorPublishResponse response) {
                    synchronized (mutex) {
                        handleResponseUnderLock(response);
                    }
                }

                private void handleResponseUnderLock(LegislatorPublishResponse response) {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    if (state.isFailed()) {
                        logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                        assert publicationCompletedIffAllTargetsInactive();
                        return;
                    }

                    if (response.getJoin().isPresent()) {
                        Join join = response.getJoin().get();
                        if (join.getTerm() == consensusState.getCurrentTerm()) {
                            handleJoin(join);
                        }
                    }
                    if (consensusState.electionWon() == false) {
                        logger.debug("PublishResponseHandler.handleResponse: stepped down as leader while publishing value {}",
                            response.getPublishResponse());
                        onPossibleCommitFailure();
                    } else if (response.getPublishResponse().getTerm() != consensusState.getCurrentTerm() ||
                        response.getPublishResponse().getVersion() != consensusState.getLastPublishedVersion()) {
                        logger.debug("PublishResponseHandler.handleResponse: [{}] is at wrong version or term {}/{} (vs {}/{})",
                            discoveryNode, response.getPublishResponse().getTerm(), response.getPublishResponse().getVersion(),
                            consensusState.getCurrentTerm(), consensusState.getLastPublishedVersion());
                        state.setState(PublicationTargetState.FAILED);
                        onPossibleCommitFailure();
                    } else {
                        state.setState(PublicationTargetState.WAITING_FOR_QUORUM);
                        handlePublishResponse(response.getPublishResponse());
                    }

                    assert publicationCompletedIffAllTargetsInactive();
                }

                @Override
                public void handleException(TransportException exp) {
                    synchronized (mutex) {
                        handleExceptionUnderLock(exp);
                    }
                }

                private void handleExceptionUnderLock(TransportException exp) {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("PublishResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("PublishResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state.setState(PublicationTargetState.FAILED);
                    onPossibleCommitFailure();
                    assert publicationCompletedIffAllTargetsInactive();
                    ackOnce(exp);
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            }

            private class ApplyCommitResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    synchronized (mutex) {
                        handleResponseUnderLock();
                    }
                }

                private void handleResponseUnderLock() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    if (state.isFailed()) {
                        logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                            discoveryNode);
                        return;
                    }
                    state.setState(PublicationTargetState.APPLIED_COMMIT);
                    onPossibleCompletion();
                    assert publicationCompletedIffAllTargetsInactive();
                    ackOnce(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    synchronized (mutex) {
                        handleExceptionUnderLock(exp);
                    }
                }

                private void handleExceptionUnderLock(TransportException exp) {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("ApplyCommitResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state.setState(PublicationTargetState.FAILED);
                    onPossibleCompletion();
                    assert publicationCompletedIffAllTargetsInactive();
                    ackOnce(exp);
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            }
        }
    }

    public void handleJoinRequest(Join join, MembershipAction.JoinCallback joinCallback) {
        synchronized (mutex) {
            handleJoinRequestUnderLock(join, joinCallback);
        }
    }

    private void handleJoinRequestUnderLock(Join join, MembershipAction.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (mode == Mode.LEADER) {
            // submit as cluster state update task
            masterService.submitStateUpdateTask("zen-disco-node-join",
                join.getSourceNode(), ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new NodeJoinController.JoinTaskListener(joinCallback, logger));
        } else {
            MembershipAction.JoinCallback prev = joinRequestAccumulator.put(join.getSourceNode(), joinCallback);
            if (prev != null) {
                prev.onFailure(new ConsensusMessageRejectedException("already have a join for " + join.getSourceNode()));
            }
        }
        handleJoin(join);
    }

    // copied from NodeJoinController.getPendingAsTasks
    private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
        joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(e.getKey(),
            new NodeJoinController.JoinTaskListener(e.getValue(), logger)));
        return tasks;
    }

    private void handleJoin(Join join) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        Optional<Join> optionalJoin = ensureTermAtLeast(localNode, join.getTerm());
        if (optionalJoin.isPresent()) {
            handleJoin(optionalJoin.get()); // someone thinks we should be master, so let's try to become one
        }

        boolean prevElectionWon = consensusState.electionWon();
        consensusState.handleJoin(join);
        assert !prevElectionWon || consensusState.electionWon(); // we cannot go from won to not won
        if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            becomeLeader("handleJoin");

            Map<DiscoveryNode, ClusterStateTaskListener> pendingAsTasks = getPendingAsTasks();
            joinRequestAccumulator.clear();

            final String source = "zen-disco-elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";
            // noop listener, the election finished listener determines result
            pendingAsTasks.put(NodeJoinController.BECOME_MASTER_TASK, (source1, e) -> {
            });
            // TODO: should we take any further action when FINISH_ELECTION_TASK fails?
            pendingAsTasks.put(NodeJoinController.FINISH_ELECTION_TASK, (source1, e) -> {
            });
            masterService.submitStateUpdateTasks(source, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }
    }

    private void removeNode(final DiscoveryNode node, final String source, final String reason) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        masterService.submitStateUpdateTask(
            source + "(" + node + "), reason(" + reason + ")",
            new NodeRemovalClusterStateTaskExecutor.Task(node, reason),
            ClusterStateTaskConfig.build(Priority.IMMEDIATE),
            nodeRemovalExecutor,
            nodeRemovalExecutor);
    }

    public LegislatorPublishResponse handlePublishRequest(PublishRequest publishRequest) {
        synchronized (mutex) {
            return handlePublishRequestUnderLock(publishRequest);
        }
    }

    private LegislatorPublishResponse handlePublishRequestUnderLock(PublishRequest publishRequest) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        DiscoveryNode sourceNode = publishRequest.getAcceptedState().nodes().getMasterNode();
        // adapt to local node
        ClusterState clusterState = ClusterState.builder(publishRequest.getAcceptedState()).nodes(
            DiscoveryNodes.builder(publishRequest.getAcceptedState().nodes()).localNodeId(getLocalNode().getId()).build()).build();
        publishRequest = new PublishRequest(clusterState);

        ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());

        logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

        final PublishResponse publishResponse = consensusState.handlePublishRequest(publishRequest);
        if (sourceNode.equals(localNode)) {
            // responder refreshes required because lastAcceptedState has changed
            leaderCheckResponder = new LeaderCheckResponder();
            heartbeatRequestResponder = new HeartbeatRequestResponder();
        } else {
            // becomeFollower refreshes responders
            becomeFollower("handlePublishRequest", sourceNode);
        }

        if (lastJoin.isPresent() && lastJoin.get().getTargetNode().getId().equals(sourceNode.getId())
            && lastJoin.get().getTerm() == publishRequest.getAcceptedState().term()) {
            return new LegislatorPublishResponse(publishResponse, lastJoin);
        }
        return new LegislatorPublishResponse(publishResponse, Optional.empty());
    }

    public HeartbeatResponse handleHeartbeatRequest(DiscoveryNode sourceNode, HeartbeatRequest heartbeatRequest) {
        return heartbeatRequestResponder.handleHeartbeatRequest(sourceNode, heartbeatRequest);
    }

    private void sendJoin(DiscoveryNode sourceNode, Join join) {
        // No synchronisation required
        transport.sendJoin(sourceNode, join, new TransportResponseHandler<Empty>() {
            @Override
            public Empty read(StreamInput in) {
                return Empty.INSTANCE;
            }

            @Override
            public void handleResponse(Empty response) {
                // No synchronisation required
                logger.debug("SendJoinResponseHandler: successfully joined {}", sourceNode);
            }

            @Override
            public void handleException(TransportException exp) {
                // No synchronisation required
                if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                    logger.debug("SendJoinResponseHandler: [{}] failed: {}", sourceNode, exp.getRootCause().getMessage());
                } else {
                    logger.debug(() -> new ParameterizedMessage("SendJoinResponseHandler: [{}] failed", sourceNode), exp);
                }
            }

            @Override
            public String executor() {
                return Names.GENERIC;
            }
        });
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            handleApplyCommitUnderLock(sourceNode, applyCommit, applyListener);
        }
    }

    private void handleApplyCommitUnderLock(DiscoveryNode sourceNode, ApplyCommit applyCommit, ActionListener<Void> applyListener) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.trace("handleApplyCommit: applying {} from [{}]", applyCommit, sourceNode);
        try {
            consensusState.handleCommit(applyCommit);
        } catch (ConsensusMessageRejectedException e) {
            applyListener.onFailure(e);
            return;
        }

        // mark state as commmitted
        lastCommittedState = Optional.of(consensusState.getLastAcceptedState());
        if (sourceNode.equals(localNode)) {
            // master node applies the committed state at the end of the publication process, not here.
            applyListener.onResponse(null);
        } else {
            clusterApplier.onNewClusterState("master [" + sourceNode + "] sent [" + applyCommit + "]", () -> getLastCommittedState().get(),
                new ClusterApplyListener() {

                    @Override
                    public void onFailure(String source, Exception e) {
                        applyListener.onFailure(e);
                    }

                    @Override
                    public void onSuccess(String source) {
                        applyListener.onResponse(null);
                    }
                });
        }
    }

    public OfferJoin handleSeekJoins(SeekJoins seekJoins) {
        synchronized (mutex) {
            return handleSeekJoinsUnderLock(seekJoins);
        }
    }

    private OfferJoin handleSeekJoinsUnderLock(SeekJoins seekJoins) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("handleSeekJoins: received [{}] from [{}]", seekJoins, seekJoins.getSourceNode());

        boolean shouldOfferJoin = false;

        if (mode == Mode.CANDIDATE) {
            shouldOfferJoin = true;
        } else if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(seekJoins.getSourceNode())) {
            // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a
            // follower. It's possible that the leader lost its quorum, but while we're still a follower we will not
            // offer joins to any other node so there is no major drawback in offering a join to our old leader. The
            // advantage of this is that it makes it slightly more likely that the leader won't change, and also that
            // its re-election will happen more quickly than if it had to wait for a quorum of followers to also detect
            // its failure.
            logger.debug("handleSeekJoins: following a failed leader");
            shouldOfferJoin = true;
        }

        if (shouldOfferJoin) {
            OfferJoin offerJoin = new Messages.OfferJoin(consensusState.getLastAcceptedVersion(),
                consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());
            logger.debug("handleSeekJoins: candidate received [{}] from [{}] and responding with [{}]", seekJoins,
                seekJoins.getSourceNode(), offerJoin);
            return offerJoin;
        } else if (consensusState.getCurrentTerm() < seekJoins.getTerm() && mode == Mode.LEADER) {
            // This is a _rare_ case that can occur if this node is the leader but pauses for long enough for other
            // nodes to consider it failed, leading to `sender` winning a pre-voting round and increments its term, but
            // then this node comes back to life and commits another value in its term, re-establishing itself as the
            // leader and preventing `sender` from winning its election. In this situation all the other nodes end up in
            // mode FOLLOWER and therefore ignore SeekJoins messages from `sender`, but `sender` ignores PublishRequest
            // messages from this node, so is effectively excluded from the cluster for this term. The solution is for
            // this node to perform an election in a yet-higher term so that `sender` can re-join the cluster.
            final long newTerm = seekJoins.getTerm() + 1;
            logger.debug("handleSeekJoins: leader in term {} handling [{}] from [{}] by starting an election in term {}",
                consensusState.getCurrentTerm(), seekJoins, seekJoins.getSourceNode(), newTerm);
            sendStartJoin(new StartJoinRequest(localNode, newTerm));
            throw new ConsensusMessageRejectedException("I'm still a leader");

            // TODO what about a node that sent a join to a different node in our term? Is it now stuck until the next term?

        } else {
            // TODO: remove this once we have a discovery layer. If a node finds an active master node during discovery,
            // it will try to join that one, and not start seeking joins.
            if (mode == Mode.LEADER) {
                if (activeLeaderFailureDetector.get().faultyNodes.remove(seekJoins.getSourceNode())) {
                    leaderCheckResponder = new LeaderCheckResponder();
                }
                masterService.submitStateUpdateTask("zen-disco-node-join",
                    seekJoins.getSourceNode(), ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor, new NodeJoinController.JoinTaskListener(new MembershipAction.JoinCallback() {
                        @Override
                        public void onSuccess() {
                            logger.trace("handleSeekJoins: initiated join of {} successful", seekJoins.getSourceNode());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.trace("handleSeekJoins: initiated join of {} failed: {}", seekJoins.getSourceNode(), e.getMessage());
                        }
                    }, logger));
            }
            logger.debug("handleSeekJoins: not offering join: lastAcceptedVersion={}, term={}, mode={}, lastKnownLeader={}",
                consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), mode, lastKnownLeader);
            throw new ConsensusMessageRejectedException("not offering join");
        }
    }

    public class OfferJoinCollector {
        final NodeCollection joinsOffered = new NodeCollection();
        long maxTermSeen = 0L;

        public void add(DiscoveryNode sender, long term) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            joinsOffered.add(sender);
            maxTermSeen = Math.max(maxTermSeen, term);
        }

        public void start(SeekJoins seekJoins) {
            // No synchronisation required, assuming that `nodeSupplier` is independently threadsafe.
            nodeSupplier.get().forEach(n -> transport.sendSeekJoins(n, seekJoins, new TransportResponseHandler<Messages.OfferJoin>() {
                @Override
                public OfferJoin read(StreamInput in) throws IOException {
                    return new Messages.OfferJoin(in);
                }

                @Override
                public void handleResponse(OfferJoin response) {
                    synchronized (mutex) {
                        handleOfferJoinUnderLock(n, OfferJoinCollector.this, response);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // no synchronisation required
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("OfferJoinCollector: [{}] failed: {}", n, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("OfferJoinCollector: [{}] failed", n), exp);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }));
        }
    }

    private void handleOfferJoinUnderLock(DiscoveryNode sender, OfferJoinCollector offerJoinCollector, OfferJoin offerJoin) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (currentOfferJoinCollector.isPresent() == false || currentOfferJoinCollector.get() != offerJoinCollector) {
            logger.debug("handleOfferJoin: received OfferJoin message from [{}] but not collecting offers.", sender);
            throw new ConsensusMessageRejectedException("Received OfferJoin but not collecting offers.");
        }

        assert currentOfferJoinCollector.get() == offerJoinCollector;

        if (offerJoin.getLastAcceptedTerm() > consensusState.getLastAcceptedTerm()
            || (offerJoin.getLastAcceptedTerm() == consensusState.getLastAcceptedTerm()
            && offerJoin.getLastAcceptedVersion() > consensusState.getLastAcceptedVersion())) {
            logger.debug("handleOfferJoin: handing over pre-voting to [{}] because of {}", sender, offerJoin);
            currentOfferJoinCollector = Optional.empty();
            transport.sendPreJoinHandover(sender, new PrejoinHandoverRequest(localNode));
            return;
        }

        logger.debug("handleOfferJoin: received {} from [{}]", offerJoin, sender);
        offerJoinCollector.add(sender, offerJoin.getTerm());

        if (consensusState.isElectionQuorum(offerJoinCollector.joinsOffered)) {
            logger.debug("handleOfferJoin: received a quorum of OfferJoin messages, so starting an election.");
            currentOfferJoinCollector = Optional.empty();
            sendStartJoin(new StartJoinRequest(localNode, Math.max(consensusState.getCurrentTerm(), offerJoinCollector.maxTermSeen) + 1));
        }
    }

    public void handlePreJoinHandover(PrejoinHandoverRequest prejoinHandoverRequest) {
        synchronized (mutex) {
            logger.debug("handlePreJoinHandover: received handover from [{}]", prejoinHandoverRequest.getSourceNode());
            startSeekingJoins();
        }
    }

    public void abdicateTo(DiscoveryNode newLeader) {
        synchronized (mutex) {
            if (mode != Mode.LEADER) {
                logger.debug("abdicateTo: mode={} != LEADER, so cannot abdicate to [{}]", mode, newLeader);
                throw new ConsensusMessageRejectedException("abdicateTo: not currently leading, so cannot abdicate.");
            }
            logger.debug("abdicateTo: abdicating to [{}]", newLeader);
        transport.sendAbdication(newLeader, new AbdicationRequest(newLeader, consensusState.getCurrentTerm()));
        }
    }

    public void handleAbdication(AbdicationRequest abdicationRequest) {
        // No synchronisation required
        logger.debug("handleAbdication: accepting abdication from [{}] in term {}", abdicationRequest.getSourceNode(),
            abdicationRequest.getTerm());
        sendStartJoin(new StartJoinRequest(localNode, abdicationRequest.getTerm() + 1));
    }

    private void sendStartJoin(StartJoinRequest startJoinRequest) {
        // No synchronisation required, assuming that `nodeSupplier` is independently threadsafe.
        nodeSupplier.get().forEach(n -> transport.sendStartJoin(n, startJoinRequest,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    // ignore
                }

                @Override
                public void handleException(TransportException exp) {
                    // No synchronisation required
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("handleStartJoinResponse: [{}] failed: {}", n, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("handleStartJoinResponse: failed to get join from [{}]", n), exp);
                    }
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            }));
    }

    public void handleDisconnectedNode(DiscoveryNode sender) {
        synchronized (mutex) {
            logger.trace("handleDisconnectedNode: lost connection to leader [{}]", sender);
            if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(sender)) {
                becomeCandidate("handleDisconnectedNode");
            }
            if (mode == Mode.LEADER) {
                activeLeaderFailureDetector.get().addFaultyNode(sender);
                removeNode(sender, "node_left", "handleDisconnectedNode");
            }
        }
    }

    public void invariant() {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                assert consensusState.electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(localNode);
            } else if (mode == Mode.FOLLOWER) {
                assert consensusState.electionWon() == false : localNode + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(localNode) == false);
            } else {
                assert mode == Mode.CANDIDATE;
            }

            assert (seekJoinsScheduler.isPresent()) == (mode == Mode.CANDIDATE);
            assert (activeFollowerFailureDetector.isPresent()) == (mode == Mode.FOLLOWER);
            assert (activeLeaderFailureDetector.isPresent()) == (mode == Mode.LEADER);
            assert (currentPublication.isPresent() == false) || (mode == Mode.LEADER);
        }
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    public interface Transport {
        void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                TransportResponseHandler<LegislatorPublishResponse> responseHandler);

        void sendHeartbeatRequest(DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                  TransportResponseHandler<HeartbeatResponse> responseHandler);

        void sendApplyCommit(DiscoveryNode destination, ApplyCommit applyCommit,
                             TransportResponseHandler<TransportResponse.Empty> responseHandler);

        void sendSeekJoins(DiscoveryNode destination, SeekJoins seekJoins, TransportResponseHandler<Messages.OfferJoin> responseHandler);

        void sendStartJoin(DiscoveryNode destination, StartJoinRequest startJoinRequest,
                           TransportResponseHandler<TransportResponse.Empty> responseHandler);

        void sendJoin(DiscoveryNode destination, Join join, TransportResponseHandler<TransportResponse.Empty> responseHandler);

        void sendPreJoinHandover(DiscoveryNode destination, PrejoinHandoverRequest prejoinHandoverRequest);

        void sendAbdication(DiscoveryNode destination, AbdicationRequest abdicationRequest);

        void sendLeaderCheckRequest(DiscoveryNode destination, LeaderCheckRequest leaderCheckRequest,
                                    TransportResponseHandler<LeaderCheckResponse> responseHandler);
    }

    public interface FutureExecutor {
        void schedule(TimeValue delay, String description, Runnable task);
    }

    private class SeekJoinsScheduler {

        private long currentDelayMillis = 0;
        private boolean running = true;

        SeekJoinsScheduler() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            scheduleNextWakeUp();
        }

        void stop() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert running;
            running = false;
        }

        @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
        private long randomNonNegativeLong() {
            // Is java.util.Random threadsafe? no synchronisation required if so.
            long result = random.nextLong();
            return result == Long.MIN_VALUE ? 0 : Math.abs(result);
        }

        private long randomLongBetween(long lowerBound, long upperBound) {
            // No extra synchronisation required
            assert 0 < upperBound - lowerBound;
            return randomNonNegativeLong() % (upperBound - lowerBound) + lowerBound;
        }

        private void scheduleNextWakeUp() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert running;
            assert mode == Mode.CANDIDATE;
            currentDelayMillis = Math.min(maxDelay.getMillis(), currentDelayMillis + minDelay.getMillis());
            final long delay = randomLongBetween(minDelay.getMillis(), currentDelayMillis + 1);
            futureExecutor.schedule(TimeValue.timeValueMillis(delay), "SeekJoinsScheduler#scheduleNextWakeUp", this::handleWakeUp);
        }

        private void handleWakeUp() {
            synchronized (mutex) {
                logger.debug("SeekJoinsScheduler.handleWakeUp: " +
                        "waking up as {} at [{}] with running={}, version={}, term={}, lastAcceptedTerm={}",
                    mode, currentTimeSupplier.getAsLong(), running,
                    consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

                if (running) {
                    scheduleNextWakeUp();
                    startSeekingJoins();
                }
            }
        }
    }

    private class ActiveLeaderFailureDetector {

        // nodes that have been detected as faulty and which are not expected to participate in publications
        private final Set<DiscoveryNode> faultyNodes = new HashSet<>();

        private final Map<DiscoveryNode, NodeFD> nodesFD = new HashMap<>();

        class NodeFD {

            private int failureCountSinceLastSuccess = 0;

            private final DiscoveryNode followerNode;

            private NodeFD(DiscoveryNode followerNode) {
                this.followerNode = followerNode;
            }

            private boolean running() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                return this.equals(nodesFD.get(followerNode));
            }

            public void start() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                assert running();
                logger.trace("ActiveLeaderFailureDetector.start: starting failure detection against {}", followerNode);
                scheduleNextWakeUp();
            }

            private void scheduleNextWakeUp() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                futureExecutor.schedule(heartbeatDelay, "ActiveLeaderFailureDetector#handleWakeUp", this::handleWakeUp);
            }

            private void handleWakeUp() {
                synchronized (mutex) {
                    logger.trace("ActiveLeaderFailureDetector.handleWakeUp: " +
                            "waking up as {} at [{}] with running={}, lastAcceptedVersion={}, term={}, lastAcceptedTerm={}",
                        mode, currentTimeSupplier.getAsLong(), running(),
                        consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

                    if (running()) {
                        assert mode == Mode.LEADER;
                        new FollowerCheck().start();
                    }
                }
            }

            private void onCheckFailure(DiscoveryNode node) {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                if (running()) {
                    failureCountSinceLastSuccess++;
                    if (failureCountSinceLastSuccess >= leaderCheckRetryCount) {
                        logger.debug("ActiveLeaderFailureDetector.onCheckFailure: {} consecutive failures to check [{}]",
                            failureCountSinceLastSuccess, node);
                        addFaultyNode(node);
                        removeNode(node, "node_left", "ActiveLeaderFailureDetector.onCheckFailure");
                    } else {
                        scheduleNextWakeUp();
                    }
                }
            }

            private void onCheckSuccess() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                if (running()) {
                    logger.trace("ActiveLeaderFailureDetector.onCheckSuccess: successful response from {}", followerNode);
                    failureCountSinceLastSuccess = 0;
                    scheduleNextWakeUp();
                }
            }

            private class FollowerCheck {

                private boolean inFlight = false;

                void start() {
                    assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                    logger.trace("FollowerCheck: sending follower check to [{}]", followerNode);
                    assert inFlight == false;
                    inFlight = true;
                    futureExecutor.schedule(heartbeatTimeout, "FollowerCheck#onTimeout", this::onTimeout);

                    HeartbeatRequest heartbeatRequest = new HeartbeatRequest(localNode,
                        consensusState.getCurrentTerm(), consensusState.getLastPublishedVersion());

                    transport.sendHeartbeatRequest(followerNode, heartbeatRequest, new TransportResponseHandler<HeartbeatResponse>() {

                        @Override
                        public HeartbeatResponse read(StreamInput in) throws IOException {
                            return new HeartbeatResponse(in);
                        }

                        @Override
                        public void handleResponse(HeartbeatResponse heartbeatResponse) {
                            synchronized (mutex) {
                                logger.trace("FollowerCheck.handleResponse: received {} from [{}]", heartbeatResponse, followerNode);
                                inFlight = false;
                                onCheckSuccess();
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            synchronized (mutex) {
                                if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                                    logger.debug("FollowerCheck.handleException: {}", exp.getRootCause().getMessage());
                                } else {
                                    logger.debug(() -> new ParameterizedMessage(
                                        "FollowerCheck.handleException: received exception from [{}]", followerNode), exp);
                                }
                                inFlight = false;
                                onCheckFailure(followerNode);
                            }
                        }

                        @Override
                        public String executor() {
                            return Names.GENERIC;
                        }
                    });
                }

                private void onTimeout() {
                    synchronized (mutex) {
                        if (inFlight) {
                            logger.debug("FollowerCheck.onTimeout: no response received from [{}]", followerNode);
                            inFlight = false;
                            onCheckFailure(followerNode);
                        }
                    }
                }
            }
        }

        private void updateNodesAndPing(ClusterState clusterState) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";

            // remove any nodes we don't need, this will cause their FD to stop
            nodesFD.keySet().removeIf(monitoredNode -> !clusterState.nodes().nodeExists(monitoredNode));
            if (faultyNodes.removeIf(monitoredNode -> !clusterState.nodes().nodeExists(monitoredNode))) {
                leaderCheckResponder = new LeaderCheckResponder();
            }

            // add any missing nodes
            for (DiscoveryNode node : clusterState.nodes()) {
                if (node.equals(localNode)) {
                    // no need to monitor the local node
                    continue;
                }
                if (nodesFD.containsKey(node) == false && faultyNodes.contains(node) == false) {
                    NodeFD fd = new NodeFD(node);
                    // it's OK to overwrite an existing nodeFD - it will just stop and the new one will pick things up.
                    nodesFD.put(node, fd);
                    fd.start();
                }
            }
        }

        public void addFaultyNode(DiscoveryNode node) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            logger.trace("ActiveLeaderFailureDetector.addFaultyNode: adding {}", node);
            if (faultyNodes.add(node)) {
                leaderCheckResponder = new LeaderCheckResponder();
            }
            nodesFD.remove(node);
            currentPublication.ifPresent(p -> p.onFaultyNode(node));
        }

        public void stop() {
            nodesFD.clear();
        }
    }

    private class ActiveFollowerFailureDetector {

        private boolean running = false;
        private int failureCountSinceLastSuccess = 0;

        public void start() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert running == false;
            running = true;
            scheduleNextWakeUp();
        }

        private void scheduleNextWakeUp() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            futureExecutor.schedule(heartbeatDelay, "ActiveFollowerFailureDetector#handleWakeUp", this::handleWakeUp);
        }

        void stop() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            assert running;
            running = false;
        }

        private void handleWakeUp() {
            synchronized (mutex) {
                logger.trace("ActiveFollowerFailureDetector.handleWakeUp: " +
                        "waking up as {} at [{}] with running={}, lastAcceptedVersion={}, term={}, lastAcceptedTerm={}",
                    mode, currentTimeSupplier.getAsLong(), running,
                    consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

                if (running) {
                    assert mode == Mode.FOLLOWER;
                    new LeaderCheck(lastKnownLeader.get()).start();
                }
            }
        }

        private void onCheckFailure(@Nullable Throwable cause) {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            if (running) {
                failureCountSinceLastSuccess++;
                if (cause instanceof MasterFaultDetection.NodeDoesNotExistOnMasterException) {
                    logger.debug("ActiveFollowerFailureDetector.onCheckFailure: node not part of the cluster");
                    becomeCandidate("ActiveFollowerFailureDetector.onCheckFailure");
                } else if (failureCountSinceLastSuccess >= leaderCheckRetryCount) {
                    logger.debug("ActiveFollowerFailureDetector.onCheckFailure: {} consecutive failures to check the leader",
                        failureCountSinceLastSuccess);
                    becomeCandidate("ActiveFollowerFailureDetector.onCheckFailure");
                } else {
                    scheduleNextWakeUp();
                }
            }
        }

        private void onCheckSuccess() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            if (running) {
                failureCountSinceLastSuccess = 0;
                scheduleNextWakeUp();
            }
        }

        private class LeaderCheck {

            private final DiscoveryNode leader;
            private boolean inFlight = false;

            LeaderCheck(DiscoveryNode leader) {
                this.leader = leader;
            }

            void start() {
                assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                logger.trace("LeaderCheck: sending leader check to [{}]", leader);
                assert inFlight == false;
                inFlight = true;
                futureExecutor.schedule(heartbeatTimeout, "LeaderCheck#onTimeout", this::onTimeout);

                transport.sendLeaderCheckRequest(leader, leaderCheckRequest, new TransportResponseHandler<LeaderCheckResponse>() {

                    @Override
                    public LeaderCheckResponse read(StreamInput in) throws IOException {
                        return new LeaderCheckResponse(in);
                    }

                    @Override
                    public void handleResponse(LeaderCheckResponse leaderCheckResponse) {
                        synchronized (mutex) {
                            handleResponseUnderLock(leaderCheckResponse);
                        }
                    }

                    private void handleResponseUnderLock(LeaderCheckResponse leaderCheckResponse) {
                        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                        logger.trace("LeaderCheck.handleResponse: received {} from [{}]", leaderCheckResponse, leader);
                        inFlight = false;
                        onCheckSuccess();

                        final long leaderVersion = leaderCheckResponse.getVersion();
                        long localVersion = getLastCommittedState().map(ClusterState::getVersion).orElse(-1L);
                        if (leaderVersion > localVersion && leaderVersion > 0 && running) {
                            logger.trace("LeaderCheck.handleResponse: heartbeat for version {} > local version {}, starting lag detector",
                                leaderVersion, localVersion);
                            futureExecutor.schedule(publishTimeout, "LeaderCheck#lagDetection", () -> {
                                long localVersion2 = getLastCommittedState().map(ClusterState::getVersion).orElse(-1L);
                                if (leaderVersion > localVersion2 && running) {
                                    synchronized (mutex) {
                                        logger.debug(
                                            "LeaderCheck.handleResponse: lag detected: local version {} < leader version {} after {}",
                                            localVersion2, leaderVersion, publishTimeout);
                                        laggingUntilCommittedVersionExceeds = Math.max(1, localVersion2);
                                        heartbeatRequestResponder = new HeartbeatRequestResponder();
                                    }
                                }
                            });
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        synchronized (mutex) {
                            handleExceptionUnderLock(exp);
                        }
                    }

                    private void handleExceptionUnderLock(TransportException exp) {
                        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
                        if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                            logger.debug("LeaderCheck.handleException: {}", exp.getRootCause().getMessage());
                        } else {
                            logger.debug(() -> new ParameterizedMessage(
                                "LeaderCheck.handleException: received exception from [{}]", leader), exp);
                        }
                        inFlight = false;
                        onCheckFailure(exp.getRootCause());
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }
                });
            }

            private void onTimeout() {
                synchronized (mutex) {
                    if (inFlight) {
                        logger.debug("LeaderCheck.onTimeout: no response received from [{}]", leader);
                        inFlight = false;
                        onCheckFailure(null);
                    }
                }
            }
        }
    }

    private class LeaderCheckResponder {
        private final Mode mode;
        private final DiscoveryNodes lastAcceptedNodes;
        private final Set<DiscoveryNode> faultyNodes;

        LeaderCheckResponder() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            mode = Legislator.this.mode;
            lastAcceptedNodes = consensusState.getLastAcceptedState().getNodes();
            faultyNodes = Collections.unmodifiableSet(
                activeLeaderFailureDetector.map(alfd -> new HashSet<>(alfd.faultyNodes)).orElse(new HashSet<>()));
        }

        private LeaderCheckResponse handleLeaderCheckRequest(LeaderCheckRequest leaderCheckRequest) {

            DiscoveryNode sender = leaderCheckRequest.getSourceNode();

            if (mode != Mode.LEADER) {
                logger.debug("handleLeaderCheckRequest: currently {}, rejecting message from [{}]", mode, sender);
                throw new ConsensusMessageRejectedException("handleLeaderCheckRequest: currently {}, rejecting message from [{}]",
                    mode, sender);
            }

            // TODO: use last published state instead of last accepted state? Where can we access that state?
            if (lastAcceptedNodes.nodeExists(sender) == false) {
                logger.debug("handleLeaderCheckRequest: rejecting message from [{}] as not publication target", sender);
                throw new MasterFaultDetection.NodeDoesNotExistOnMasterException();
            }

            if (faultyNodes.contains(sender)) {
                logger.debug("handleLeaderCheckRequest: rejecting message from [{}] as it is faulty", sender);
                throw new MasterFaultDetection.NodeDoesNotExistOnMasterException();
            }

            LeaderCheckResponse response = new LeaderCheckResponse(consensusState.getLastPublishedVersion());
            logger.trace("handleLeaderCheckRequest: responding to [{}] with {}", sender, response);
            return response;
        }
    }

    private class HeartbeatRequestResponder {
        private final Mode mode;
        private final Optional<DiscoveryNode> lastKnownLeader;
        private final long currentTerm;
        private final long laggingUntilCommittedVersionExceeds;
        private final long lastAcceptedVersion;

        HeartbeatRequestResponder() {
            assert Thread.holdsLock(mutex) : "Legislator mutex not held";
            mode = Legislator.this.mode;
            lastKnownLeader = Legislator.this.lastKnownLeader;
            currentTerm = consensusState.getCurrentTerm();
            laggingUntilCommittedVersionExceeds = Legislator.this.laggingUntilCommittedVersionExceeds;
            lastAcceptedVersion = consensusState.getLastAcceptedVersion();
        }

        HeartbeatResponse handleHeartbeatRequest(DiscoveryNode sourceNode, HeartbeatRequest heartbeatRequest) {
            logger.trace("handleHeartbeatRequest: handling [{}] from [{}])", heartbeatRequest, sourceNode);
            assert sourceNode.equals(localNode) == false; // localNode is final & immutable so this is ok

            if (currentTerm < heartbeatRequest.getTerm()
                || mode != Mode.FOLLOWER
                || Optional.of(sourceNode).equals(lastKnownLeader) == false) {
                return handleHeartbeatRequestUpdatingState(sourceNode, heartbeatRequest);
            }

            if (heartbeatRequest.getTerm() < currentTerm) {
                logger.debug("handleHeartbeatRequest: rejecting [{}] from [{}] as current term is {}",
                    heartbeatRequest, sourceNode, currentTerm);
                throw new ConsensusMessageRejectedException("HeartbeatRequest rejected: required term <= {} but current term is {}",
                    heartbeatRequest.getTerm(), currentTerm);
            }

            Optional<ClusterState> lastCommittedState = Legislator.this.lastCommittedState; // volatile read of lastCommittedState is ok

            if (laggingUntilCommittedVersionExceeds > 0
                && (lastCommittedState.isPresent() == false || lastCommittedState.get().version() <= laggingUntilCommittedVersionExceeds)) {
                logger.debug("handleHeartbeatRequest: rejecting [{}] from [{}] due to lag at version [{}]",
                    heartbeatRequest, sourceNode, laggingUntilCommittedVersionExceeds);
                throw new ConsensusMessageRejectedException("HeartbeatRequest rejected: lagging at version [{}]",
                    laggingUntilCommittedVersionExceeds);
            }

            return new HeartbeatResponse(lastAcceptedVersion, currentTerm);
        }

        private HeartbeatResponse handleHeartbeatRequestUpdatingState(DiscoveryNode sourceNode, HeartbeatRequest heartbeatRequest) {
            synchronized (mutex) {
                ensureTermAtLeast(sourceNode, heartbeatRequest.getTerm()).ifPresent(join -> {
                    logger.debug("handleHeartbeatRequest: sending join [{}] for term [{}] to {}",
                        join, heartbeatRequest.getTerm(), sourceNode);
                    sendJoin(sourceNode, join);
                });

                if (consensusState.getCurrentTerm() != heartbeatRequest.getTerm()) {
                    logger.debug("handleHeartbeatRequest: rejecting [{}] from [{}] as current term is {}",
                        heartbeatRequest, sourceNode, currentTerm);
                    throw new ConsensusMessageRejectedException("HeartbeatRequest rejected: requested term is {} but current term is {}",
                        heartbeatRequest.getTerm(), currentTerm);
                }

                becomeFollower("handleHeartbeatRequest", sourceNode);

                return new HeartbeatResponse(consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm());
            }
        }
    }
}

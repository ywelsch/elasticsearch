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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.NodeJoinController;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private Optional<SeekJoinsScheduler> seekJoinsScheduler;
    private Optional<ActiveLeaderFailureDetector> activeLeaderFailureDetector;
    private Optional<ActiveFollowerFailureDetector> activeFollowerFailureDetector;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect join offers.
    private Optional<OfferJoinCollector> currentOfferJoinCollector;

    private Optional<ClusterState> lastCommittedState; // the state that was last committed, can be exposed to the cluster state applier

    // similar to NodeJoinController.ElectionContext.joinRequestAccumulator, captures joins on election
    private final Map<DiscoveryNode, MembershipAction.JoinCallback> joinRequestAccumulator = new HashMap<>();

    public Legislator(Settings settings, ConsensusState.PersistedState persistedState,
                      Transport transport, MasterService masterService, DiscoveryNode localNode, LongSupplier currentTimeSupplier,
                      FutureExecutor futureExecutor, Supplier<List<DiscoveryNode>> nodeSupplier) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        leaderCheckRetryCount = CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING.get(settings);
        heartbeatDelay = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(settings);
        heartbeatTimeout = CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(settings);
        publishTimeout = CONSENSUS_PUBLISH_TIMEOUT_SETTING.get(settings);
        random = Randomness.get();

        consensusState = new ConsensusState(settings, persistedState);
        this.transport = transport;
        this.masterService = masterService;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.futureExecutor = futureExecutor;
        this.nodeSupplier = nodeSupplier;
        lastKnownLeader = Optional.empty();
        lastCommittedState = Optional.empty();
        lastJoin = Optional.empty();
        currentOfferJoinCollector = Optional.empty();
        seekJoinsScheduler = Optional.empty();
        activeLeaderFailureDetector = Optional.empty();
        activeFollowerFailureDetector = Optional.empty();

        becomeCandidate("init");

        assert localNode.equals(persistedState.getLastAcceptedState().nodes().getLocalNode()) :
            "local node mismatch, expected " + localNode + " but got " + persistedState.getLastAcceptedState().nodes().getLocalNode();
    }

    public Mode getMode() {
        return mode;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public void handleFailure() {
        if (mode == Mode.CANDIDATE) {
            logger.debug("handleFailure: already a candidate");
        } else {
            becomeCandidate("handleFailure");
        }
    }

    private void becomeCandidate(String method) {
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;

            clearJoins();
            assert seekJoinsScheduler.isPresent() == false;
            seekJoinsScheduler = Optional.of(new SeekJoinsScheduler());

            stopActiveLeaderFailureDetector();
            stopActiveFollowerFailureDetector();
        }
    }

    private void becomeLeader(String method) {
        assert mode != Mode.LEADER;

        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;

        assert activeLeaderFailureDetector.isPresent() == false;
        final ActiveLeaderFailureDetector activeLeaderFailureDetector = new ActiveLeaderFailureDetector();
        this.activeLeaderFailureDetector = Optional.of(activeLeaderFailureDetector);
        stopSeekJoinsScheduler();
        stopActiveFollowerFailureDetector();
        lastKnownLeader = Optional.of(localNode);
    }

    private void becomeFollower(String method, DiscoveryNode leaderNode) {
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
    }

    private void stopSeekJoinsScheduler() {
        if (seekJoinsScheduler.isPresent()) {
            seekJoinsScheduler.get().stop();
            seekJoinsScheduler = Optional.empty();
        }
    }

    private void stopActiveLeaderFailureDetector() {
        if (activeLeaderFailureDetector.isPresent()) {
            activeLeaderFailureDetector.get().stop();
            activeLeaderFailureDetector = Optional.empty();
        }
    }

    private void stopActiveFollowerFailureDetector() {
        if (activeFollowerFailureDetector.isPresent()) {
            activeFollowerFailureDetector.get().stop();
            activeFollowerFailureDetector = Optional.empty();
        }
    }

    private void clearJoins() {
        joinRequestAccumulator.values().forEach(
            joinCallback -> joinCallback.onFailure(new ConsensusMessageRejectedException("node stepped down as leader")));
        joinRequestAccumulator.clear();
    }

    private void startSeekingJoins() {
        clearJoins(); // TODO: is this the right place?

        currentOfferJoinCollector = Optional.of(new OfferJoinCollector());
        SeekJoins seekJoins = new SeekJoins(consensusState.getCurrentTerm(), consensusState.getLastAcceptedVersion());
        currentOfferJoinCollector.get().start(seekJoins);
    }

    private Join joinLeaderInTerm(DiscoveryNode sourceNode, long term) {
        logger.debug("joinLeaderInTerm: from [{}] with term {}", sourceNode, term);
        Join join = consensusState.handleStartJoin(sourceNode, term);
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("joinLeaderInTerm");
        }
        return join;
    }

    public void handleStartJoin(DiscoveryNode sourceNode, StartJoinRequest startJoinRequest) {
        Join join = joinLeaderInTerm(sourceNode, startJoinRequest.getTerm());

        transport.sendJoin(sourceNode, join, new TransportResponseHandler<TransportResponse.Empty>() {
            @Override
            public void handleResponse(TransportResponse.Empty response) {
                logger.debug("SendJoinResponseHandler: successfully joined {}", sourceNode);
            }

            @Override
            public void handleException(TransportException exp) {
                if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                    logger.debug("SendJoinResponseHandler: [{}] failed: {}", sourceNode, exp.getRootCause().getMessage());
                } else {
                    logger.debug(() -> new ParameterizedMessage("SendJoinResponseHandler: [{}] failed", sourceNode), exp);
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(sourceNode, targetTerm));
        }
        return Optional.empty();
    }

    public void handleClientValue(ClusterState clusterState) {
        if (mode != Mode.LEADER) {
            throw new ConsensusMessageRejectedException("handleClientValue: not currently leading, so cannot handle client value.");
        }
        PublishRequest publishRequest = consensusState.handleClientValue(clusterState);
        publish(publishRequest);
    }

    public LeaderCheckResponse handleLeaderCheckRequest(DiscoveryNode sender) {
        if (mode != Mode.LEADER) {
            logger.debug("handleLeaderCheckRequest: currently {}, rejecting message from [{}]", mode, sender);
            throw new ConsensusMessageRejectedException("handleLeaderCheckRequest: currently {}, rejecting message from [{}]",
                mode, sender);
        }

        // TODO: use last published state instead of last accepted state? Where can we access that state?
        if (consensusState.getLastAcceptedState().getNodes().nodeExists(sender) == false) {
            logger.debug("handleLeaderCheckRequest: rejecting message from [{}] as not publication target", sender);
            throw new MasterFaultDetection.NodeDoesNotExistOnMasterException();
        }

        LeaderCheckResponse response = new LeaderCheckResponse(consensusState.getLastPublishedVersion());
        logger.trace("handleLeaderCheckRequest: responding to [{}] with {}", sender, response);
        return response;
    }

    private void publish(PublishRequest publishRequest) {
        final Publication publication = new Publication(publishRequest);
        futureExecutor.schedule(publishTimeout, "Publication#onTimeout", publication::onTimeout);
        activeLeaderFailureDetector.get().updateNodesAndPing(publishRequest.getAcceptedState());
        publication.start();
    }

    public ClusterState getLastAcceptedState() {
        return consensusState.getLastAcceptedState();
    }

    public long getCurrentTerm() {
        return consensusState.getCurrentTerm();
    }

    public Optional<ClusterState> getLastCommittedState() {
        return lastCommittedState;
    }

    public boolean hasElectionQuorum(VotingConfiguration votingConfiguration) {
        return consensusState.hasElectionQuorum(votingConfiguration);
    }

    public enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        WAITING_FOR_QUORUM,
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
    }

    /**
     * A single attempt to publish an update
     */
    private class Publication {

        private final AtomicReference<ApplyCommit> applyCommitReference;
        private final List<PublicationTarget> publicationTargets;
        private final PublishRequest publishRequest;

        private Publication(PublishRequest publishRequest) {
            this.publishRequest = publishRequest;
            applyCommitReference = new AtomicReference<>();

            publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
            publishRequest.getAcceptedState().getNodes().iterator().forEachRemaining(n -> publicationTargets.add(new PublicationTarget(n)));
        }

        private void start() {
            logger.trace("publishing {} to {}", publishRequest, publicationTargets);
            publicationTargets.forEach(PublicationTarget::sendPublishRequest);
            onPossibleCommitFailure();
        }

        private void onCommitted(final ApplyCommit applyCommit) {
            assert applyCommitReference.get() == null;
            applyCommitReference.set(applyCommit);
            publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum).forEach(PublicationTarget::sendApplyCommit);
        }

        private void onPossibleCommitFailure() {
            if (applyCommitReference.get() != null) {
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

        private void failActiveTargets() {
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(PublicationTarget::setFailed);
        }

        public void onTimeout() {
            failActiveTargets();

            if (mode == Mode.LEADER && applyCommitReference.get() == null) {
                becomeCandidate("Publication.onTimeout()");
            }
        }

        private class PublicationTarget {
            private final DiscoveryNode discoveryNode;
            private final PublicationTargetStateMachine state = new PublicationTargetStateMachine();

            private PublicationTarget(DiscoveryNode discoveryNode) {
                this.discoveryNode = discoveryNode;
            }

            @Override
            public String toString() {
                return discoveryNode.getId();
            }

            public void sendPublishRequest() {
                state.setState(PublicationTargetState.SENT_PUBLISH_REQUEST);
                transport.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
                // TODO Can this ^ fail with an exception? Target should be failed if so.
            }

            void handlePublishResponse(PublishResponse publishResponse) {
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
                state.setState(PublicationTargetState.SENT_APPLY_COMMIT);

                ApplyCommit applyCommit = applyCommitReference.get();
                assert applyCommit != null;

                transport.sendApplyCommit(discoveryNode, applyCommit, new ApplyCommitResponseHandler());
            }

            public boolean isWaitingForQuorum() {
                return state.isWaitingForQuorum();
            }

            public boolean isActive() {
                return state.isActive();
            }

            public void setFailed() {
                assert isActive();
                state.setState(PublicationTargetState.FAILED);
            }

            private class PublicationTargetStateMachine {
                private PublicationTargetState state = PublicationTargetState.NOT_STARTED;

                public void setState(PublicationTargetState newState) {
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
                    return state != PublicationTargetState.FAILED
                        && state != PublicationTargetState.APPLIED_COMMIT;
                }

                public boolean isWaitingForQuorum() {
                    return state == PublicationTargetState.WAITING_FOR_QUORUM;
                }

                public boolean mayCommitInFuture() {
                    return (state == PublicationTargetState.NOT_STARTED
                        || state == PublicationTargetState.SENT_PUBLISH_REQUEST
                        || state == PublicationTargetState.WAITING_FOR_QUORUM);
                }

                public boolean isFailed() {
                    return state == PublicationTargetState.FAILED;
                }

                @Override
                public String toString() {
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
                    if (state.isFailed()) {
                        logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                        return;
                    }

                    if (response.getJoin().isPresent()) {
                        Join join = response.getJoin().get();
                        if (join.getTerm() == consensusState.getCurrentTerm()) {
                            handleJoin(discoveryNode, join);
                        }
                    }
                    if (consensusState.electionWon() == false) {
                        logger.debug("PublishResponseHandler.handleResponse: stepped down as leader before committing value {}",
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
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("PublishResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("PublishResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state.setState(PublicationTargetState.FAILED);
                    onPossibleCommitFailure();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }

            private class ApplyCommitResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    if (state.isFailed()) {
                        logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                            discoveryNode);
                        return;
                    }
                    state.setState(PublicationTargetState.APPLIED_COMMIT);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("ApplyCommitResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state.setState(PublicationTargetState.FAILED);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        }
    }

    public void handleJoinRequest(DiscoveryNode sourceNode, Join join, MembershipAction.JoinCallback joinCallback) {
        if (mode == Mode.LEADER) {
            // submit as cluster state update task
            masterService.submitTask(join.toString(),
                clusterState -> joinNodes(clusterState, Collections.singletonList(sourceNode)).resultingState);
        } else {
            MembershipAction.JoinCallback prev = joinRequestAccumulator.put(sourceNode, joinCallback);
            if (prev != null) {
                prev.onFailure(new ConsensusMessageRejectedException("already have a join for " + sourceNode));
            }
        }
        handleJoin(sourceNode, join);
    }

    // copied from NodeJoinController.getPendingAsTasks
    private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
        Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
        joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(e.getKey(),
            new NodeJoinController.JoinTaskListener(e.getValue(), logger)));
        return tasks;
    }

    private void handleJoin(DiscoveryNode sourceNode, Join join) {
        Optional<Join> optionalJoin = ensureTermAtLeast(localNode, join.getTerm());
        if (optionalJoin.isPresent()) {
            handleJoin(localNode, optionalJoin.get()); // someone thinks we should be master, so let's try to become one
        }

        boolean prevElectionWon = consensusState.electionWon();
        consensusState.handleJoin(sourceNode, join);
        assert !prevElectionWon || consensusState.electionWon(); // we cannot go from won to not won
        if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            becomeLeader("handleJoin");

            Map<DiscoveryNode, ClusterStateTaskListener> pendingAsTasks = getPendingAsTasks();
            joinRequestAccumulator.clear();

            masterService.submitTask(join.toString(), clusterState ->
                joinNodes(clusterState, pendingAsTasks.keySet().stream().collect(Collectors.toList())).resultingState);
        }
    }

    // copied from NodeJoinController.JoinTaskExecutor.execute(...)
    public ClusterTasksResult<DiscoveryNode> joinNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
        final ClusterTasksResult.Builder<DiscoveryNode> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        ClusterState.Builder newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        assert nodesBuilder.isLocalNodeElectedMaster();

        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // we only enforce major version transitions on a fully formed clusters
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        for (final DiscoveryNode node : joiningNodes) {
            if (currentNodes.nodeExists(node)) {
                logger.debug("received a join request for an existing node [{}]", node);
            } else {
                try {
                    if (enforceMajorVersion) {
                        MembershipAction.ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    MembershipAction.ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                    // we have to reject nodes that don't support all indices we have in this cluster
                    MembershipAction.ensureIndexCompatibility(node.getVersion(), currentState.getMetaData());
                    nodesBuilder.add(node);
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    results.failure(node, e);
                    continue;
                }
            }
            results.success(node);
        }
        newState.nodes(nodesBuilder);
        return results.build(newState.build());
    }

    // copied from NodeJoinController.JoinTaskExecutor.becomeMasterAndTrimConflictingNodes(...)
    private ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final DiscoveryNode joiningNode : joiningNodes) {
            final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
            if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                nodesBuilder.remove(nodeWithSameId.getId());
            }
            final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
            if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                    joiningNode);
                nodesBuilder.remove(nodeWithSameAddress.getId());
            }
        }

        return ClusterState.builder(currentState).nodes(nodesBuilder);
    }

    // copied from ZenDiscovery.NodeRemovalClusterStateTaskExecutor.execute(...)
    public ClusterTasksResult<DiscoveryNode> removeNodes(ClusterState currentState, List<DiscoveryNode> nodes) {
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false;
        for (final DiscoveryNode node : nodes) {
            if (currentState.nodes().nodeExists(node)) {
                remainingNodesBuilder.remove(node);
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", node);
            }
        }

        if (!removed) {
            // no nodes to remove, keep the current cluster state
            return ClusterTasksResult.<DiscoveryNode>builder().successes(nodes).build(currentState);
        }

        final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

        final ClusterTasksResult.Builder<DiscoveryNode> resultBuilder = ClusterTasksResult.<DiscoveryNode>builder().successes(nodes);
        return resultBuilder.build(remainingNodesClusterState);
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

    public LegislatorPublishResponse handlePublishRequest(DiscoveryNode sourceNode, PublishRequest publishRequest) {
        // adapt to local node
        ClusterState clusterState = ClusterState.builder(publishRequest.getAcceptedState()).nodes(
            DiscoveryNodes.builder(publishRequest.getAcceptedState().nodes()).localNodeId(getLocalNode().getId()).build()).build();
        publishRequest = new PublishRequest(clusterState);

        final Optional<Join> optionalJoin = ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());

        if (optionalJoin.isPresent()) {
            lastJoin = optionalJoin;
        }

        logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

        final PublishResponse publishResponse = consensusState.handlePublishRequest(publishRequest);
        if (sourceNode.equals(localNode) == false) {
            becomeFollower("handlePublishRequest", sourceNode);
        }

        if (lastJoin.isPresent() && lastJoin.get().getTargetNode().getId().equals(sourceNode.getId())
            && lastJoin.get().getTerm() == publishRequest.getAcceptedState().term()) {
            return new LegislatorPublishResponse(publishResponse, lastJoin);
        }
        return new LegislatorPublishResponse(publishResponse, Optional.empty());
    }

    public HeartbeatResponse handleHeartbeatRequest(DiscoveryNode sourceNode, HeartbeatRequest heartbeatRequest) {
        logger.trace("handleHeartbeatRequest: handling [{}] from [{}])", heartbeatRequest, sourceNode);
        assert sourceNode.equals(localNode) == false;

        if (heartbeatRequest.getTerm() < consensusState.getCurrentTerm()) {
            logger.debug("handleHeartbeatRequest: rejecting [{}] from [{}] as current term is {}",
                heartbeatRequest, sourceNode, consensusState.getCurrentTerm());
            throw new ConsensusMessageRejectedException("HeartbeatRequest rejected: required term <= {} but current term is {}",
                heartbeatRequest.getTerm(), consensusState.getCurrentTerm());
        }

        ensureTermAtLeast(sourceNode, heartbeatRequest.getTerm()).ifPresent(join -> {
            logger.debug("handleHeartbeatRequest: sending join [{}] for term [{}] to {}",
                join, heartbeatRequest.getTerm(), sourceNode);

            transport.sendJoin(sourceNode, join, new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    logger.debug("SendJoinResponseHandler: successfully joined {}", sourceNode);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("SendJoinResponseHandler: [{}] failed: {}", sourceNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("SendJoinResponseHandler: [{}] failed", sourceNode), exp);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        });

        becomeFollower("handleHeartbeatRequest", sourceNode);

        return new HeartbeatResponse(consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm());
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit) {
        logger.trace("handleApplyCommit: applying {} from [{}]", applyCommit, sourceNode);
        consensusState.handleCommit(applyCommit);
        // mark state as commmitted
        lastCommittedState = Optional.of(consensusState.getLastAcceptedState());
    }

    public OfferJoin handleSeekJoins(DiscoveryNode sender, SeekJoins seekJoins) {
        logger.debug("handleSeekJoins: received [{}] from [{}]", seekJoins, sender);

        boolean shouldOfferJoin = false;

        if (mode == Mode.CANDIDATE) {
            shouldOfferJoin = true;
        } else if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(sender)) {
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
            logger.debug("handleSeekJoins: candidate received [{}] from [{}] and responding with [{}]", seekJoins, sender, offerJoin);
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
                consensusState.getCurrentTerm(), seekJoins, sender, newTerm);
            sendStartJoin(new StartJoinRequest(newTerm));
            throw new ConsensusMessageRejectedException("I'm still a leader");
        } else {
            // TODO: remove this once we have a discovery layer. If a node finds an active master node during discovery,
            // it will try to join that one, and not start seeking joins.
            if (mode == Mode.LEADER) {
                masterService.submitTask("join of " + sender,
                    clusterState -> joinNodes(clusterState, Collections.singletonList(sender)).resultingState);
            }
            logger.debug("handleSeekJoins: not offering join: lastAcceptedVersion={}, term={}, mode={}, lastKnownLeader={}",
                consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), mode, lastKnownLeader);
            throw new ConsensusMessageRejectedException("not offering join");
        }
    }

    public class OfferJoinCollector {
        NodeCollection joinsOffered = new NodeCollection();
        long maxTermSeen = 0L;

        public void add(DiscoveryNode sender, long term) {
            joinsOffered.add(sender);
            maxTermSeen = Math.max(maxTermSeen, term);
        }

        public void start(SeekJoins seekJoins) {
            nodeSupplier.get().forEach(n -> transport.sendSeekJoins(n, seekJoins, new TransportResponseHandler<Messages.OfferJoin>() {
                @Override
                public OfferJoin read(StreamInput in) throws IOException {
                    return new Messages.OfferJoin(in);
                }

                @Override
                public void handleResponse(OfferJoin response) {
                    handleOfferJoin(n, OfferJoinCollector.this, response);
                }

                @Override
                public void handleException(TransportException exp) {
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

    public void handleOfferJoin(DiscoveryNode sender, OfferJoinCollector offerJoinCollector, OfferJoin offerJoin) {
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
            transport.sendPreJoinHandover(sender);
            return;
        }

        logger.debug("handleOfferJoin: received {} from [{}]", offerJoin, sender);
        offerJoinCollector.add(sender, offerJoin.getTerm());

        if (consensusState.isElectionQuorum(offerJoinCollector.joinsOffered)) {
            logger.debug("handleOfferJoin: received a quorum of OfferJoin messages, so starting an election.");
            currentOfferJoinCollector = Optional.empty();
            sendStartJoin(new StartJoinRequest(Math.max(consensusState.getCurrentTerm(), offerJoinCollector.maxTermSeen) + 1));
        }
    }

    public void handlePreJoinHandover(DiscoveryNode sender) {
        logger.debug("handlePreJoinHandover: received handover from [{}]", sender);
        startSeekingJoins();
    }

    public void abdicateTo(DiscoveryNode newLeader) {
        if (mode != Mode.LEADER) {
            logger.debug("abdicateTo: mode={} != LEADER, so cannot abdicate to [{}]", mode, newLeader);
            throw new ConsensusMessageRejectedException("abdicateTo: not currently leading, so cannot abdicate.");
        }
        logger.debug("abdicateTo: abdicating to [{}]", newLeader);
        transport.sendAbdication(newLeader, consensusState.getCurrentTerm());
    }

    public void handleAbdication(DiscoveryNode sender, long currentTerm) {
        logger.debug("handleAbdication: accepting abdication from [{}] in term {}", sender, currentTerm);
        sendStartJoin(new StartJoinRequest(currentTerm + 1));
    }

    private void sendStartJoin(StartJoinRequest startStartJoinRequest) {
        nodeSupplier.get().forEach(n -> transport.sendStartJoin(n, startStartJoinRequest,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) throws IOException {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    // ignore
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("handleStartJoinResponse: [{}] failed: {}", n, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("handleStartJoinResponse: failed to get join from [{}]", n), exp);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }));
    }

    public void handleDisconnectedNode(DiscoveryNode sender) {
        logger.trace("handleDisconnectedNode: lost connection to leader [{}]", sender);
        if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(sender)) {
            becomeCandidate("handleDisconnectedNode");
        }
        if (mode == Mode.LEADER) {
            masterService.submitTask("disconnect from " + sender,
                clusterState -> removeNodes(clusterState, Collections.singletonList(sender)).resultingState);
        }
    }

    public void invariant() {
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

        void sendPreJoinHandover(DiscoveryNode destination);

        void sendAbdication(DiscoveryNode destination, long currentTerm);

        void sendLeaderCheckRequest(DiscoveryNode discoveryNode, TransportResponseHandler<LeaderCheckResponse> responseHandler);
    }

    public interface FutureExecutor {
        void schedule(TimeValue delay, String description, Runnable task);
    }

    private class SeekJoinsScheduler {

        private long currentDelayMillis = 0;
        private boolean running = true;

        SeekJoinsScheduler() {
            scheduleNextWakeUp();
        }

        void stop() {
            assert running;
            running = false;
        }

        @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
        private long randomNonNegativeLong() {
            long result = random.nextLong();
            return result == Long.MIN_VALUE ? 0 : Math.abs(result);
        }

        private long randomLongBetween(long lowerBound, long upperBound) {
            assert 0 < upperBound - lowerBound;
            return randomNonNegativeLong() % (upperBound - lowerBound) + lowerBound;
        }

        private void scheduleNextWakeUp() {
            assert running;
            assert mode == Mode.CANDIDATE;
            currentDelayMillis = Math.min(maxDelay.getMillis(), currentDelayMillis + minDelay.getMillis());
            final long delay = randomLongBetween(minDelay.getMillis(), currentDelayMillis + 1);
            futureExecutor.schedule(TimeValue.timeValueMillis(delay), "SeekJoinsScheduler#scheduleNextWakeUp", this::handleWakeUp);
        }

        private void handleWakeUp() {
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

    private class ActiveLeaderFailureDetector {

        private final Map<DiscoveryNode, NodeFD> nodesFD = new HashMap<>();

        class NodeFD {

            private int failureCountSinceLastSuccess = 0;

            private final DiscoveryNode followerNode;

            private NodeFD(DiscoveryNode followerNode) {
                this.followerNode = followerNode;
            }

            private boolean running() {
                return this.equals(nodesFD.get(followerNode));
            }

            public void start() {
                assert running();
                logger.trace("ActiveLeaderFailureDetector.start: starting failure detection against {}", followerNode);
                scheduleNextWakeUp();
            }

            private void scheduleNextWakeUp() {
                futureExecutor.schedule(heartbeatDelay, "ActiveLeaderFailureDetector#handleWakeUp", this::handleWakeUp);
            }

            private void handleWakeUp() {
                logger.trace("ActiveLeaderFailureDetector.handleWakeUp: " +
                        "waking up as {} at [{}] with running={}, lastAcceptedVersion={}, term={}, lastAcceptedTerm={}",
                    mode, currentTimeSupplier.getAsLong(), running(),
                    consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

                if (running()) {
                    assert mode == Mode.LEADER;
                    new FollowerCheck().start();
                }
            }

            private void onCheckFailure(DiscoveryNode node) {
                if (running()) {
                    failureCountSinceLastSuccess++;
                    if (failureCountSinceLastSuccess >= leaderCheckRetryCount) {
                        logger.debug("ActiveFollowerFailureDetector.onCheckFailure: {} consecutive failures to check the leader",
                            failureCountSinceLastSuccess);
                        masterService.submitTask("node fault detection kicked out " + node,
                            clusterState -> removeNodes(clusterState, Collections.singletonList(node)).resultingState);
                    } else {
                        scheduleNextWakeUp();
                    }
                }
            }

            private void onCheckSuccess() {
                if (running()) {
                    logger.trace("ActiveLeaderFailureDetector.onCheckSuccess: successful response from {}", followerNode);
                    failureCountSinceLastSuccess = 0;
                    scheduleNextWakeUp();
                }
            }

            private class FollowerCheck {

                private boolean inFlight = false;

                void start() {
                    logger.trace("FollowerCheck: sending follower check to [{}]", followerNode);
                    assert inFlight == false;
                    inFlight = true;
                    futureExecutor.schedule(heartbeatTimeout, "FollowerCheck#onTimeout", this::onTimeout);

                    HeartbeatRequest heartbeatRequest = new HeartbeatRequest(
                        consensusState.getCurrentTerm(), consensusState.getLastPublishedVersion());

                    transport.sendHeartbeatRequest(followerNode, heartbeatRequest, new TransportResponseHandler<HeartbeatResponse>() {
                        @Override
                        public void handleResponse(HeartbeatResponse heartbeatResponse) {
                            logger.trace("FollowerCheck.handleResponse: received {} from [{}]", heartbeatResponse, followerNode);
                            inFlight = false;
                            onCheckSuccess();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                                logger.debug("FollowerCheck.handleException: {}", exp.getRootCause().getMessage());
                            } else {
                                logger.debug(() -> new ParameterizedMessage(
                                    "FollowerCheck.handleException: received exception from [{}]", followerNode), exp);
                            }
                            inFlight = false;
                            onCheckFailure(followerNode);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
                }

                private void onTimeout() {
                    if (inFlight) {
                        logger.debug("FollowerCheck.onTimeout: no response received from [{}]", followerNode);
                        inFlight = false;
                        onCheckFailure(followerNode);
                    }
                }
            }
        }

        public void updateNodesAndPing(ClusterState clusterState) {
            // remove any nodes we don't need, this will cause their FD to stop
            nodesFD.keySet().removeIf(monitoredNode -> !clusterState.nodes().nodeExists(monitoredNode));

            // add any missing nodes
            for (DiscoveryNode node : clusterState.nodes()) {
                if (node.equals(localNode)) {
                    // no need to monitor the local node
                    continue;
                }
                if (!nodesFD.containsKey(node)) {
                    NodeFD fd = new NodeFD(node);
                    // it's OK to overwrite an existing nodeFD - it will just stop and the new one will pick things up.
                    nodesFD.put(node, fd);
                    fd.start();
                }
            }
        }

        public void stop() {
            nodesFD.clear();
        }
    }

    private class ActiveFollowerFailureDetector {

        private boolean running = false;
        private int failureCountSinceLastSuccess = 0;

        public void start() {
            assert running == false;
            running = true;
            scheduleNextWakeUp();
        }

        private void scheduleNextWakeUp() {
            futureExecutor.schedule(heartbeatDelay, "ActiveFollowerFailureDetector#handleWakeUp", this::handleWakeUp);
        }

        void stop() {
            assert running;
            running = false;
        }

        private void handleWakeUp() {
            logger.trace("ActiveFollowerFailureDetector.handleWakeUp: " +
                    "waking up as {} at [{}] with running={}, lastAcceptedVersion={}, term={}, lastAcceptedTerm={}",
                mode, currentTimeSupplier.getAsLong(), running,
                consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

            if (running) {
                assert mode == Mode.FOLLOWER;
                new LeaderCheck(lastKnownLeader.get()).start();
            }
        }

        private void onCheckFailure(@Nullable Throwable cause) {
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
                logger.trace("LeaderCheck: sending leader check to [{}]", leader);
                assert inFlight == false;
                inFlight = true;
                futureExecutor.schedule(heartbeatTimeout, "LeaderCheck#onTimeout", this::onTimeout);

                transport.sendLeaderCheckRequest(leader, new TransportResponseHandler<LeaderCheckResponse>() {
                    @Override
                    public void handleResponse(LeaderCheckResponse leaderCheckResponse) {
                        logger.trace("LeaderCheck.handleResponse: received {} from [{}]", leaderCheckResponse, leader);
                        inFlight = false;
                        onCheckSuccess();

                        final long leaderVersion = leaderCheckResponse.getVersion();
                        long localVersion = getLastCommittedState().map(ClusterState::getVersion).orElse(-1L);
                        if (leaderVersion > localVersion) {
                            logger.trace("LeaderCheck.handleResponse: heartbeat for version {} > local version {}, starting lag detector",
                                leaderVersion, localVersion);
                            futureExecutor.schedule(publishTimeout, "LeaderCheck#lagDetection", () -> {
                                long localVersion2 = getLastCommittedState().map(ClusterState::getVersion).orElse(-1L);
                                if (leaderVersion > localVersion2) {
                                    logger.debug("LeaderCheck.handleResponse: lag detected: local version {} < leader version {} after {}",
                                        localVersion2, leaderVersion, publishTimeout);
                                    becomeCandidate("LeaderCheck.handleResponse");
                                }
                            });
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
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
                        return ThreadPool.Names.SAME;
                    }
                });
            }

            private void onTimeout() {
                if (inFlight) {
                    logger.debug("LeaderCheck.onTimeout: no response received from [{}]", leader);
                    inFlight = false;
                    onCheckFailure(null);
                }
            }
        }
    }

    public interface MasterService {
        void submitTask(String reason, Function<ClusterState, ClusterState> runnable);
    }
}

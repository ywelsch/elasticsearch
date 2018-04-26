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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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
    private Optional<HeartbeatScheduler> heartbeatScheduler;
    private Optional<ActiveFollowerFailureDetector> activeFollowerFailureDetector;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect join offers.
    private Optional<OfferJoinCollector> currentOfferJoinCollector;

    private Optional<ClusterState> lastCommittedState; // the state that was last committed, can be exposed to the cluster state applier

    public Legislator(Settings settings, ConsensusState.PersistedState persistedState,
                      Transport transport, DiscoveryNode localNode, LongSupplier currentTimeSupplier,
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
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.futureExecutor = futureExecutor;
        this.nodeSupplier = nodeSupplier;
        lastKnownLeader = Optional.empty();
        lastCommittedState = Optional.empty();
        lastJoin = Optional.empty();
        currentOfferJoinCollector = Optional.empty();
        seekJoinsScheduler = Optional.empty();
        heartbeatScheduler = Optional.empty();
        activeFollowerFailureDetector = Optional.empty();

        becomeCandidate("init");
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

            assert seekJoinsScheduler.isPresent() == false;
            seekJoinsScheduler = Optional.of(new SeekJoinsScheduler());

            stopHeartbeatScheduler();
            stopActiveFollowerFailureDetector();
        }
    }

    private void becomeLeader(String method) {
        assert mode != Mode.LEADER;

        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;

        assert heartbeatScheduler.isPresent() == false;
        final HeartbeatScheduler heartbeatScheduler = new HeartbeatScheduler();
        this.heartbeatScheduler = Optional.of(heartbeatScheduler);
        heartbeatScheduler.start();
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
        stopHeartbeatScheduler();
    }

    private void stopSeekJoinsScheduler() {
        if (seekJoinsScheduler.isPresent()) {
            seekJoinsScheduler.get().stop();
            seekJoinsScheduler = Optional.empty();
        }
    }

    private void stopHeartbeatScheduler() {
        if (heartbeatScheduler.isPresent()) {
            heartbeatScheduler.get().stop();
            heartbeatScheduler = Optional.empty();
        }
    }

    private void stopActiveFollowerFailureDetector() {
        if (activeFollowerFailureDetector.isPresent()) {
            activeFollowerFailureDetector.get().stop();
            activeFollowerFailureDetector = Optional.empty();
        }
    }

    private void startSeekingJoins() {
        currentOfferJoinCollector = Optional.of(new OfferJoinCollector());
        SeekJoins seekJoins = new SeekJoins(consensusState.getCurrentTerm(), consensusState.getLastAcceptedVersion());
        currentOfferJoinCollector.get().start(seekJoins);
    }

    public Join handleStartJoin(DiscoveryNode sourceNode, StartJoinRequest startJoinRequest) {
        logger.debug("handleStartJoin: from [{}] with term {}", sourceNode, startJoinRequest.getTerm());
        Join join = consensusState.handleStartJoin(sourceNode, startJoinRequest.getTerm());
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("handleStartJoin");
        }
        return join;
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            return Optional.of(handleStartJoin(sourceNode, new StartJoinRequest(targetTerm)));
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

        // TODO reject if sender is not a publication target

        LeaderCheckResponse response = new LeaderCheckResponse(consensusState.getLastAcceptedVersion());
        logger.trace("handleLeaderCheckRequest: responding to [{}] with {}", sender, response);
        return response;
    }

    private void publish(PublishRequest publishRequest) {
        final Publication publication = new Publication(publishRequest);
        futureExecutor.schedule(publishTimeout, publication::onTimeout);
        publication.start();
    }

    public ClusterState getLastAcceptedState() {
        return consensusState.getLastAcceptedState();
    }

    public Optional<ClusterState> getLastCommittedState() {
        return lastCommittedState;
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

            final List<DiscoveryNode> discoveryNodes = nodeSupplier.get();
            publicationTargets = new ArrayList<>(discoveryNodes.size());
            discoveryNodes.stream().map(PublicationTarget::new).forEach(publicationTargets::add);
        }

        private void start() {
            publicationTargets.forEach(PublicationTarget::sendPublishRequest);
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
                if (publicationTarget.state == PublicationTargetState.SENT_PUBLISH_REQUEST
                    || publicationTarget.state == PublicationTargetState.NOT_STARTED
                    || publicationTarget.state == PublicationTargetState.WAITING_FOR_QUORUM) {

                    possiblySuccessfulNodes.add(publicationTarget.discoveryNode);
                } else {
                    assert publicationTarget.state == PublicationTargetState.FAILED : publicationTarget.state;
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
            private PublicationTargetState state;

            private PublicationTarget(DiscoveryNode discoveryNode) {
                this.discoveryNode = discoveryNode;
                state = PublicationTargetState.NOT_STARTED;
            }

            public void sendPublishRequest() {
                assert state == PublicationTargetState.NOT_STARTED : state;
                state = PublicationTargetState.SENT_PUBLISH_REQUEST;
                transport.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
                // TODO Can this ^ fail with an exception? Target should be failed if so.
            }

            void handlePublishResponse(PublishResponse publishResponse) {
                assert state == PublicationTargetState.WAITING_FOR_QUORUM : state;

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
                assert state == PublicationTargetState.WAITING_FOR_QUORUM : state;
                state = PublicationTargetState.SENT_APPLY_COMMIT;

                ApplyCommit applyCommit = applyCommitReference.get();
                assert applyCommit != null;

                transport.sendApplyCommit(discoveryNode, applyCommit, new ApplyCommitResponseHandler());
            }

            public boolean isWaitingForQuorum() {
                return state == PublicationTargetState.WAITING_FOR_QUORUM;
            }

            public boolean isActive() {
                return state != PublicationTargetState.FAILED
                    && state != PublicationTargetState.APPLIED_COMMIT;
            }

            public void setFailed() {
                assert isActive();
                state = PublicationTargetState.FAILED;
            }

            private class PublishResponseHandler implements TransportResponseHandler<LegislatorPublishResponse> {
                @Override
                public LegislatorPublishResponse read(StreamInput in) throws IOException {
                    return new LegislatorPublishResponse(in);
                }

                @Override
                public void handleResponse(LegislatorPublishResponse response) {
                    if (state == PublicationTargetState.FAILED) {
                        logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                        return;
                    }

                    assert state == PublicationTargetState.SENT_PUBLISH_REQUEST : state;

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
                        state = PublicationTargetState.FAILED;
                        onPossibleCommitFailure();
                    } else {
                        state = PublicationTargetState.WAITING_FOR_QUORUM;
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
                    state = PublicationTargetState.FAILED;
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
                    if (state == PublicationTargetState.FAILED) {
                        logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                            discoveryNode);
                        return;
                    }

                    assert state == PublicationTargetState.SENT_APPLY_COMMIT : state;
                    state = PublicationTargetState.APPLIED_COMMIT;
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("ApplyCommitResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state = PublicationTargetState.FAILED;
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        }
    }

    void handleJoin(DiscoveryNode sourceNode, Join join) {
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

            ClusterState noop = createNoop(consensusState.getCurrentTerm(), consensusState.getLastAcceptedState());
            PublishRequest publishRequest = consensusState.handleClientValue(noop);
            publish(publishRequest);
        }
    }

    private static ClusterState createNoop(long term, ClusterState clusterState) {
        return ClusterState.builder(clusterState).incrementVersion().term(term).build();
    }

    public LegislatorPublishResponse handlePublishRequest(DiscoveryNode sourceNode, PublishRequest publishRequest) {
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
            // TODO: remove this once it's taken care of by fault detection
            if (mode == Mode.LEADER) {
                ClusterState state = createNoop(consensusState.getCurrentTerm(), consensusState.getLastAcceptedState());
                PublishRequest publishRequest = consensusState.handleClientValue(state);
                publish(publishRequest);
            }
            logger.debug("handleSeekJoins: not offering join: lastAcceptedVersion={}, term={}, mode={}",
                consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), mode);
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
        nodeSupplier.get().forEach(n -> transport.sendStartJoin(n, startStartJoinRequest, new TransportResponseHandler<Join>() {
            @Override
            public Join read(StreamInput in) throws IOException {
                return new Join(in);
            }

            @Override
            public void handleResponse(Join response) {
                handleJoin(n, response);
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
        assert (heartbeatScheduler.isPresent()) == (mode == Mode.LEADER);
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

        void sendStartJoin(DiscoveryNode destination, StartJoinRequest startJoinRequest, TransportResponseHandler<Join> responseHandler);

        void sendPreJoinHandover(DiscoveryNode destination);

        void sendAbdication(DiscoveryNode destination, long currentTerm);

        void sendLeaderCheckRequest(DiscoveryNode discoveryNode, TransportResponseHandler<LeaderCheckResponse> responseHandler);
    }

    public interface FutureExecutor {
        void schedule(TimeValue delay, Runnable task);
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
            futureExecutor.schedule(TimeValue.timeValueMillis(delay), this::handleWakeUp);
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

    private class HeartbeatScheduler {

        private boolean running = false;
        private final long term; // for assertions that a new term gets a new scheduler

        HeartbeatScheduler() {
            term = consensusState.getCurrentTerm();
        }

        void start() {
            logger.trace("HeartbeatScheduler[{}].start(): starting", term);
            assert running == false;
            running = true;
            scheduleNextWakeUp();
        }

        void stop() {
            assert running;
            running = false;
        }

        private void scheduleNextWakeUp() {
            assert running;
            assert mode == Mode.LEADER;
            assert consensusState.getCurrentTerm() == term
                : "HeartbeatScheduler#term = " + term + " != consensusState#term = " + consensusState.getCurrentTerm();
            futureExecutor.schedule(heartbeatDelay, this::handleWakeUp);
        }

        private void handleWakeUp() {
            logger.trace("HeartbeatScheduler.handleWakeUp: " +
                    "waking up as {} at [{}] with running={}, lastAcceptedVersion={}, term={}, lastAcceptedTerm={}",
                mode, currentTimeSupplier.getAsLong(), running,
                consensusState.getLastAcceptedVersion(), consensusState.getCurrentTerm(), consensusState.getLastAcceptedTerm());

            if (running) {
                scheduleNextWakeUp();
                new Heartbeat();
            }
        }

        private class Heartbeat {

            final List<DiscoveryNode> allNodes = new ArrayList<>(nodeSupplier.get());
            final List<DiscoveryNode> successfulNodes = new ArrayList<>(allNodes.size());
            final List<DiscoveryNode> failedNodes = new ArrayList<>(allNodes.size());

            boolean receivedQuorum = false;
            boolean failed = false;

            Heartbeat() {
                final HeartbeatRequest heartbeatRequest
                    = new HeartbeatRequest(consensusState.getCurrentTerm(), consensusState.getLastAcceptedVersion());

                futureExecutor.schedule(heartbeatTimeout, this::onTimeout);

                nodeSupplier.get().forEach(n -> {
                    if (n.equals(localNode) == false) {
                        logger.trace("Heartbeat: sending heartbeat to {}", n);
                        transport.sendHeartbeatRequest(n, heartbeatRequest,
                            new TransportResponseHandler<HeartbeatResponse>() {
                                @Override
                                public HeartbeatResponse read(StreamInput in) throws IOException {
                                    return new HeartbeatResponse(in);
                                }

                                @Override
                                public void handleResponse(HeartbeatResponse heartbeatResponse) {
                                    logger.trace("Heartbeat.handleResponse: received [{}] from [{}]", heartbeatResponse, n);
                                    assert heartbeatResponse.getTerm() <= term;
                                    if (heartbeatResponse.getVersion() > consensusState.getLastAcceptedVersion()) {
                                        logger.debug("Heartbeat.handleResponse: follower has a later version than the leader's {}",
                                            consensusState.getLastAcceptedVersion());
                                        becomeCandidate("Heartbeat.handleResponse");
                                    }
                                    successfulNodes.add(n);
                                    onPossibleCompletion();
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                                        logger.debug("Heartbeat.handleException: failed to get heartbeat from [{}]: {}", n,
                                            exp.getRootCause().getMessage());
                                    } else {
                                        logger.debug(() -> new ParameterizedMessage(
                                                "Heartbeat.handleException: failed to get heartbeat from [{}]", n), exp);
                                    }
                                    failedNodes.add(n);
                                    onPossibleCompletion();
                                }

                                @Override
                                public String executor() {
                                    return ThreadPool.Names.SAME;
                                }
                            });
                    }
                });

                successfulNodes.add(localNode);
                onPossibleCompletion();
            }

            private void onPossibleCompletion() {
                assert running == false || consensusState.getCurrentTerm() == term;

                if (running && receivedQuorum == false && failed == false) {
                    NodeCollection nodeCollection = new NodeCollection();
                    successfulNodes.forEach(nodeCollection::add);

                    if (consensusState.isPublishQuorum(nodeCollection)) {
                        logger.trace("Heartbeat.onPossibleCompletion: received a quorum of responses");
                        receivedQuorum = true;
                        return;
                    }

                    for (DiscoveryNode discoveryNode : allNodes) {
                        if (failedNodes.contains(discoveryNode) == false) {
                            nodeCollection.add(discoveryNode);
                        }
                    }

                    if (consensusState.isPublishQuorum(nodeCollection) == false) {
                        logger.debug("Heartbeat.onPossibleCompletion: non-failed nodes do not form a quorum");
                        failed = true;
                        becomeCandidate("Heartbeat.onPossibleCompletion");
                    }
                }
            }

            private void onTimeout() {
                assert running == false || consensusState.getCurrentTerm() == term;

                if (running && receivedQuorum == false && failed == false) {
                    logger.debug("Heartbeat.onTimeout: timed out waiting for responses");
                    becomeCandidate("Heartbeat.onTimeout");
                    failed = true;
                }
            }
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
            futureExecutor.schedule(heartbeatDelay, this::handleWakeUp);
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

        private void onCheckFailure() {
            if (running) {
                failureCountSinceLastSuccess++;
                if (failureCountSinceLastSuccess >= leaderCheckRetryCount) {
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
                futureExecutor.schedule(heartbeatTimeout, this::onTimeout);

                transport.sendLeaderCheckRequest(leader, new TransportResponseHandler<LeaderCheckResponse>() {
                    @Override
                    public void handleResponse(LeaderCheckResponse leaderCheckResponse) {
                        logger.trace("LeaderCheck.handleResponse: received {} from [{}]", leaderCheckResponse, leader);
                        inFlight = false;
                        onCheckSuccess();

                        final long leaderVersion = leaderCheckResponse.getVersion();
                        if (leaderVersion > consensusState.getLastAcceptedVersion()) {
                            logger.trace("LeaderCheck.handleResponse: heartbeat for version {} > local version {}, starting lag detector",
                                leaderVersion, consensusState.getLastAcceptedVersion());
                            futureExecutor.schedule(publishTimeout, () -> {
                                if (leaderVersion > consensusState.getLastAcceptedVersion()) {
                                    logger.debug("LeaderCheck.handleResponse: lag detected: local version {} < leader version {} after {}",
                                        leaderVersion, consensusState.getLastAcceptedVersion(), publishTimeout);
                                    becomeCandidate("LeaderCheck.handleResponse");
                                }
                            });
                        }                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                            logger.debug("LeaderCheck.handleException: {}", exp.getRootCause().getMessage());
                        } else {
                            logger.debug(() -> new ParameterizedMessage(
                                    "LeaderCheck.handleException: received exception from [{}]", leader), exp);
                        }
                        inFlight = false;
                        onCheckFailure();
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
                    onCheckFailure();
                }
            }
        }
    }
}

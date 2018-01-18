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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.ConsensusState.CommittedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.CatchupRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferVote;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.StartVoteRequest;
import org.elasticsearch.discovery.zen2.Messages.Vote;
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
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A Legislator, following the Paxos metaphor, is responsible for making sure that actions happen in a timely fashion
 * and for turning messages from the outside world into actions performed on the ConsensusState.
 *
 * @param <T> The state tracked in the replicated state machine.
 */
public class Legislator<T extends CommittedState> extends AbstractComponent {

    public static final Setting<TimeValue> CONSENSUS_MIN_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.min_delay",
            TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CONSENSUS_MAX_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.max_delay",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1000), Setting.Property.NodeScope);
    // the time in follower state without receiving new values or heartbeats from a leader before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_FOLLOWER_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.follower_timeout",
            TimeValue.timeValueMillis(90000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
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

    private final ConsensusState<T> consensusState;
    private final Transport<T> transport;
    private final DiscoveryNode localNode;
    private final Supplier<List<DiscoveryNode>> nodeSupplier;
    private final Function<T, Diff<T>> noOpCreator;
    private final TimeValue minDelay;
    private final TimeValue maxDelay;
    private final TimeValue followerTimeout;
    private final TimeValue heartbeatDelay;
    private final TimeValue heartbeatTimeout;
    private final TimeValue publishTimeout;
    private final FutureExecutor futureExecutor;
    private final LongSupplier currentTimeSupplier;
    private final Random random;

    private boolean termIncrementedAtLeastOnce; // Ensures we cannot win an election without publishing being permitted

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<SeekVotesScheduler> seekVotesScheduler;
    private Optional<HeartbeatScheduler> heartbeatScheduler;
    private Optional<PassiveFollowerFailureDetector> passiveFollowerFailureDetector;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect vote offers.
    private Optional<OfferVoteCollector> currentOfferVoteCollector;

    // Present if we are catching-up.
    private Optional<PublishRequest<T>> storedPublishRequest = Optional.empty();

    public Legislator(Settings settings, ConsensusState.PersistedState<T> persistedState,
                      Transport<T> transport, DiscoveryNode localNode, LongSupplier currentTimeSupplier,
                      FutureExecutor futureExecutor, Supplier<List<DiscoveryNode>> nodeSupplier, Function<T, Diff<T>> noOpCreator) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        followerTimeout = CONSENSUS_FOLLOWER_TIMEOUT_SETTING.get(settings);
        heartbeatDelay = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(settings);
        heartbeatTimeout = CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(settings);
        publishTimeout = CONSENSUS_PUBLISH_TIMEOUT_SETTING.get(settings);
        random = Randomness.get();

        consensusState = new ConsensusState<>(settings, persistedState);
        this.transport = transport;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.futureExecutor = futureExecutor;
        this.nodeSupplier = nodeSupplier;
        this.noOpCreator = noOpCreator;
        lastKnownLeader = Optional.empty();
        currentOfferVoteCollector = Optional.empty();
        seekVotesScheduler = Optional.empty();
        heartbeatScheduler = Optional.empty();
        passiveFollowerFailureDetector = Optional.empty();

        becomeCandidate("init");
    }

    public Mode getMode() {
        return mode;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public T getCommittedState() {
        return consensusState.getCommittedState();
    }

    public void handleFailure() {
        if (mode == Mode.CANDIDATE) {
            logger.debug("handleFailure: already a candidate");
        } else {
            becomeCandidate("handleFailure");
        }
    }

    private void becomeCandidate(String method) {
        logger.debug("{}: becoming candidate (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;

            assert seekVotesScheduler.isPresent() == false;
            seekVotesScheduler = Optional.of(new SeekVotesScheduler());

            stopHeartbeatScheduler();
            stopPassiveFollowerFailureDetector();
        }
    }

    private void becomeLeader(String method) {
        assert mode != Mode.LEADER;

        logger.debug("{}: becoming LEADER] (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;

        assert heartbeatScheduler.isPresent() == false;
        heartbeatScheduler = Optional.of(new HeartbeatScheduler());
        stopSeekVotesScheduler();
        stopPassiveFollowerFailureDetector();
        lastKnownLeader = Optional.of(localNode);
    }

    private void becomeOrRenewFollower(String method, DiscoveryNode leaderNode) {
        if (mode != Mode.FOLLOWER) {
            logger.debug("{}: becoming follower of [{}] (was {}, lastKnownLeader was [{}])", method, leaderNode, mode, lastKnownLeader);

            assert passiveFollowerFailureDetector.isPresent() == false;
        }

        mode = Mode.FOLLOWER;
        lastKnownLeader = Optional.of(leaderNode);
        stopSeekVotesScheduler();
        stopHeartbeatScheduler();
        stopPassiveFollowerFailureDetector();
        passiveFollowerFailureDetector = Optional.of(new PassiveFollowerFailureDetector());
    }

    private void stopSeekVotesScheduler() {
        if (seekVotesScheduler.isPresent()) {
            seekVotesScheduler.get().stop();
            seekVotesScheduler = Optional.empty();
        }
    }

    private void stopHeartbeatScheduler() {
        if (heartbeatScheduler.isPresent()) {
            heartbeatScheduler.get().stop();
            heartbeatScheduler = Optional.empty();
        }
    }

    private void stopPassiveFollowerFailureDetector() {
        if (passiveFollowerFailureDetector.isPresent()) {
            passiveFollowerFailureDetector.get().stop();
            passiveFollowerFailureDetector = Optional.empty();
        }
    }

    private void startSeekingVotes() {
        currentOfferVoteCollector = Optional.of(new OfferVoteCollector());
        SeekVotes seekVotes = new SeekVotes(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
        currentOfferVoteCollector.get().start(seekVotes);
    }

    public Vote handleStartVote(DiscoveryNode sourceNode, StartVoteRequest startVoteRequest) {
        logger.debug("handleStartVote: from [{}] with term {}", sourceNode, startVoteRequest.getTerm());
        Vote vote = consensusState.handleStartVote(startVoteRequest.getTerm());
        termIncrementedAtLeastOnce = true;
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("handleStartVote");
        }
        return vote;
    }

    private Optional<Vote> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            return Optional.of(handleStartVote(sourceNode, new StartVoteRequest(targetTerm)));
        }
        return Optional.empty();
    }

    public void handleClientValue(Diff<T> diff) {
        if (mode != Mode.LEADER) {
            throw new ConsensusMessageRejectedException("handleClientValue: not currently leading, so cannot handle client value.");
        }
        PublishRequest<T> publishRequest = consensusState.handleClientValue(diff);
        publish(publishRequest);
    }

    private void publish(PublishRequest<T> publishRequest) {
        final Publication publication = new Publication(publishRequest);
        futureExecutor.schedule(publishTimeout, publication::onTimeout);
        publication.start();
    }

    public enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        WAITING_FOR_QUORUM,
        SENT_CATCH_UP,
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
        ALREADY_COMMITTED
    }

    /**
     * A single attempt to publish an update
     */
    private class Publication {

        private final CatchupRequest<T> catchUp;
        private final AtomicReference<ApplyCommit> applyCommitReference;
        private final List<PublicationTarget> publicationTargets;
        private final PublishRequest<T> publishRequest;

        private Publication(PublishRequest<T> publishRequest) {
            this.publishRequest = publishRequest;
            catchUp = new CatchupRequest<>(publishRequest.getTerm(), consensusState.generateCatchup());
            assert catchUp.getFullState().getSlot() + 1 == publishRequest.getSlot();
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
                    || publicationTarget.state == PublicationTargetState.SENT_CATCH_UP
                    || publicationTarget.state == PublicationTargetState.WAITING_FOR_QUORUM) {

                    possiblySuccessfulNodes.add(publicationTarget.discoveryNode);
                } else {
                    assert publicationTarget.state == PublicationTargetState.FAILED
                        || publicationTarget.state == PublicationTargetState.ALREADY_COMMITTED;
                }
            }

            if (false == consensusState.isQuorumInCurrentConfiguration(possiblySuccessfulNodes)) {
                logger.debug("onPossibleCommitFailure: non-failed nodes do not form a quorum, so publication cannot succeed");
                failActiveTargets();
                becomeCandidate("Publication.onPossibleCommitFailure");
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
                assert state == PublicationTargetState.NOT_STARTED;
                state = PublicationTargetState.SENT_PUBLISH_REQUEST;
                transport.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
                // TODO Can this ^ fail with an exception? Target should be failed if so.
            }

            void handlePublishResponse(PublishResponse publishResponse) {
                assert state == PublicationTargetState.WAITING_FOR_QUORUM;

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
                assert state == PublicationTargetState.WAITING_FOR_QUORUM;
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
                    && state != PublicationTargetState.APPLIED_COMMIT
                    && state != PublicationTargetState.ALREADY_COMMITTED;
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

                    assert state == PublicationTargetState.SENT_PUBLISH_REQUEST;

                    if (response.getVote().isPresent()) {
                        handleVote(discoveryNode, response.getVote().get());
                    }
                    if (response.getFirstUncommittedSlot() < publishRequest.getSlot()) {
                        logger.debug("PublishResponseHandler.handleResponse: [{}] is at older slot {} (vs {}), sending catch-up",
                            discoveryNode, response.getFirstUncommittedSlot(), publishRequest.getSlot());
                        state = PublicationTargetState.SENT_CATCH_UP;
                        transport.sendCatchUp(discoveryNode, catchUp, new CatchUpResponseHandler());
                    } else if (response.getFirstUncommittedSlot() > publishRequest.getSlot()) {
                        logger.debug("PublishResponseHandler.handleResponse: [{}] is at newer slot {} (vs {}), marking ALREADY_COMMITTED",
                            discoveryNode, response.getFirstUncommittedSlot(), publishRequest.getSlot());
                        assert false == response.getPublishResponse().isPresent();
                        state = PublicationTargetState.ALREADY_COMMITTED;
                        onPossibleCommitFailure();
                    } else {
                        assert response.getPublishResponse().isPresent();
                        assert response.getFirstUncommittedSlot() == publishRequest.getSlot();
                        state = PublicationTargetState.WAITING_FOR_QUORUM;
                        handlePublishResponse(response.getPublishResponse().get());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("PublishResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                                "PublishResponseHandler: [{}] failed", discoveryNode), exp);
                    }
                    state = PublicationTargetState.FAILED;
                    onPossibleCommitFailure();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

            }

            private class CatchUpResponseHandler implements TransportResponseHandler<PublishResponse> {
                @Override
                public PublishResponse read(StreamInput in) throws IOException {
                    return new PublishResponse(in);
                }

                @Override
                public void handleResponse(PublishResponse response) {
                    if (state == PublicationTargetState.FAILED) {
                        logger.debug("CatchUpResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                        return;
                    }

                    assert state == PublicationTargetState.SENT_CATCH_UP;
                    state = PublicationTargetState.WAITING_FOR_QUORUM;
                    handlePublishResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("CatchUpResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "CatchUpResponseHandler: [{}] failed", discoveryNode), exp);
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

                    assert state == PublicationTargetState.SENT_APPLY_COMMIT;
                    state = PublicationTargetState.APPLIED_COMMIT;
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("ApplyCommitResponseHandler: [{}] failed: {}", discoveryNode, exp.getRootCause().getMessage());
                    } else {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
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

    void handleVote(DiscoveryNode sourceNode, Vote vote) {
        Optional<Vote> optionalVote = ensureTermAtLeast(localNode, vote.getTerm());
        if (optionalVote.isPresent()) {
            handleVote(localNode, optionalVote.get()); // someone thinks we should be master, so let's try to become one
        }

        if (termIncrementedAtLeastOnce == false) {
            return;
        }

        boolean prevElectionWon = consensusState.electionWon();
        Optional<PublishRequest<T>> maybePublishRequest = consensusState.handleVote(sourceNode, vote);
        assert !prevElectionWon || consensusState.electionWon(); // we cannot go from won to not won
        if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            becomeLeader("handleVote");

            if (maybePublishRequest.isPresent()) {
                publish(maybePublishRequest.get());
            } else {
                assert consensusState.canHandleClientValue();
                publish(consensusState.handleClientValue(noOpCreator.apply(consensusState.getCommittedState())));
            }
        }
    }

    public LegislatorPublishResponse handlePublishRequest(DiscoveryNode sourceNode, PublishRequest<T> publishRequest) {
        if (publishRequest.getTerm() < consensusState.getCurrentTerm()) {
            throw new ConsensusMessageRejectedException("incoming term too old");
        }

        final Optional<Vote> optionalVote = ensureTermAtLeast(sourceNode, publishRequest.getTerm());
        assert publishRequest.getTerm() == consensusState.getCurrentTerm();

        if (publishRequest.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: received request [{}] with future slot (expected [{}]): catching up",
                publishRequest, consensusState.firstUncommittedSlot());
            if (mode == Mode.LEADER) {
                // we are a stale leader with same term as new leader and have not committed configuration change yet that tells us
                // we're cannot become leader anymore.
                // TODO: Disable electionWon on this node and turn it into follower
                assert false;
            }
            if (sourceNode.equals(localNode) == false) {
                becomeOrRenewFollower("handlePublishRequest", sourceNode);
            }
            storedPublishRequest = Optional.of(publishRequest);
            return new LegislatorPublishResponse(consensusState.firstUncommittedSlot(), Optional.empty(), optionalVote);
        }

        if (publishRequest.getSlot() < consensusState.firstUncommittedSlot()) {
            logger.trace("handlePublishRequest: not handling [{}] from [{}], earlier slot than {}",
                publishRequest, sourceNode, consensusState.firstUncommittedSlot());
            return new LegislatorPublishResponse(consensusState.firstUncommittedSlot(), Optional.empty(), optionalVote);
        }

        assert publishRequest.getSlot() == consensusState.firstUncommittedSlot();

        logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

        final PublishResponse publishResponse = consensusState.handlePublishRequest(publishRequest);
        if (sourceNode.equals(localNode) == false) {
            becomeOrRenewFollower("handlePublishRequest", sourceNode);
        }

        return new LegislatorPublishResponse(consensusState.firstUncommittedSlot(), Optional.of(publishResponse), optionalVote);
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

        if (matchesNextSlot(heartbeatRequest)) {
            // TODO why only if matchesNextSlot()?
            becomeOrRenewFollower("handleHeartbeatRequest", sourceNode);
        }

        return new HeartbeatResponse(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit) {
        logger.trace("handleApplyCommit: applying {} from [{}]", applyCommit, sourceNode);

        boolean prevElectionWon = consensusState.electionWon();
        consensusState.handleCommit(applyCommit);
        if (prevElectionWon && consensusState.electionWon() && mode == Mode.LEADER) {
            logger.trace("handleApplyCommit: renewing leader lease");
            assert consensusState.canHandleClientValue();
        } else if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode != Mode.LEADER : "expected non-leader but was leader";
            assert consensusState.canHandleClientValue();
            becomeLeader("handleApplyCommit");
        } else if (prevElectionWon && consensusState.electionWon() == false) {
            assert mode == Mode.LEADER || mode == Mode.CANDIDATE : localNode.getId() + ": expected leader or candidate but was " + mode;
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("handleApplyCommit");
            }
        }
    }

    public OfferVote handleSeekVotes(DiscoveryNode sender, SeekVotes seekVotes) {
        logger.debug("handleSeekVotes: received [{}] from [{}]", seekVotes, sender);

        boolean shouldOfferVote = false;

        if (mode == Mode.CANDIDATE) {
            shouldOfferVote = true;
        } else if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(sender)) {
            // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a
            // follower. It's possible that the leader lost its quorum, but while we're still a follower we will not
            // offer votes to any other node so there is no major drawback in offering a vote to our old leader. The
            // advantage of this is that it makes it slightly more likely that the leader won't change, and also that
            // its re-election will happen more quickly than if it had to wait for a quorum of followers to also detect
            // its failure.
            logger.debug("handleSeekVotes: following a failed leader");
            shouldOfferVote = true;
        }

        if (shouldOfferVote) {
            OfferVote offerVote = new OfferVote(consensusState.firstUncommittedSlot(),
                consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
            logger.debug("handleSeekVotes: candidate received [{}] from [{}] and responding with [{}]", seekVotes, sender, offerVote);
            return offerVote;
        } else if (consensusState.getCurrentTerm() < seekVotes.getTerm() && mode == Mode.LEADER) {
            // This is a _rare_ case that can occur if this node is the leader but pauses for long enough for other
            // nodes to consider it failed, leading to `sender` winning a pre-voting round and increments its term, but
            // then this node comes back to life and commits another value in its term, re-establishing itself as the
            // leader and preventing `sender` from winning its election. In this situation all the other nodes end up in
            // mode FOLLOWER and therefore ignore SeekVotes messages from `sender`, but `sender` ignores PublishRequest
            // messages from this node, so is effectively excluded from the cluster for this term. The solution is for
            // this node to perform an election in a yet-higher term so that `sender` can re-join the cluster.
            final long newTerm = seekVotes.getTerm() + 1;
            logger.debug("handleSeekVotes: leader in term {} handling [{}] from [{}] by starting an election in term {}",
                consensusState.getCurrentTerm(), seekVotes, sender, newTerm);
            sendStartVote(new StartVoteRequest(newTerm));
            throw new ConsensusMessageRejectedException("I'm still a leader");
        } else {
            // TODO: remove this once it's taken care of by fault detection
            if (mode == Mode.LEADER && consensusState.canHandleClientValue()) {
                publish(consensusState.handleClientValue(noOpCreator.apply(consensusState.getCommittedState())));
            }
            logger.debug("handleSeekVotes: not offering vote: slot={}, term={}, mode={}",
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), mode);
            throw new ConsensusMessageRejectedException("not offering vote");
        }
    }

    public class OfferVoteCollector {
        NodeCollection votesOffered = new NodeCollection();
        long maxTermSeen = 0L;

        public void add(DiscoveryNode sender, long term) {
            votesOffered.add(sender);
            maxTermSeen = Math.max(maxTermSeen, term);
        }

        public void start(SeekVotes seekVotes) {
            nodeSupplier.get().forEach(n -> transport.sendSeekVotes(n, seekVotes, new TransportResponseHandler<OfferVote>() {
                @Override
                public OfferVote read(StreamInput in) throws IOException {
                    return new OfferVote(in);
                }

                @Override
                public void handleResponse(OfferVote response) {
                    handleOfferVote(n, OfferVoteCollector.this, response);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                        logger.debug("OfferVoteCollector: [{}] failed: {}", n, exp.getRootCause().getMessage());
                    } else {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "OfferVoteCollector: [{}] failed", n), exp);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }));
        }
    }

    public void handleOfferVote(DiscoveryNode sender, OfferVoteCollector offerVoteCollector, OfferVote offerVote) {
        if (currentOfferVoteCollector.isPresent() == false || currentOfferVoteCollector.get() != offerVoteCollector) {
            logger.debug("handleOfferVote: received OfferVote message from [{}] but not collecting offers.", sender);
            throw new ConsensusMessageRejectedException("Received OfferVote but not collecting offers.");
        }

        assert currentOfferVoteCollector.get() == offerVoteCollector;

        if (offerVote.getFirstUncommittedSlot() > consensusState.firstUncommittedSlot()
            || (offerVote.getFirstUncommittedSlot() == consensusState.firstUncommittedSlot()
            && offerVote.getLastAcceptedTerm() > consensusState.lastAcceptedTerm())) {
            logger.debug("handleOfferVote: handing over pre-voting to [{}] because of {}", sender, offerVote);
            currentOfferVoteCollector = Optional.empty();
            transport.sendPreVoteHandover(sender);
            return;
        }

        logger.debug("handleOfferVote: received {} from [{}]", offerVote, sender);
        offerVoteCollector.add(sender, offerVote.getTerm());

        if (consensusState.isQuorumInCurrentConfiguration(offerVoteCollector.votesOffered)) {
            logger.debug("handleOfferVote: received a quorum of OfferVote messages, so starting an election.");
            currentOfferVoteCollector = Optional.empty();
            sendStartVote(new StartVoteRequest(Math.max(consensusState.getCurrentTerm(), offerVoteCollector.maxTermSeen) + 1));
        }
    }

    public void handlePreVoteHandover(DiscoveryNode sender) {
        logger.debug("handlePreVoteHandover: received handover from [{}]", sender);
        startSeekingVotes();
    }

    public boolean matchesNextSlot(Messages.SlotTerm slotTerm) {
        return slotTerm.getTerm() == consensusState.getCurrentTerm() &&
            slotTerm.getSlot() == consensusState.firstUncommittedSlot();
    }

    public PublishResponse handleCatchUp(DiscoveryNode sender, CatchupRequest<T> catchUp) {
        logger.debug("handleCatchUp: received catch-up from {} to slot {}", sender, catchUp.getFullState().getSlot());
        if (catchUp.getTerm() != consensusState.getCurrentTerm()) {
            throw new ConsensusMessageRejectedException("catchup for wrong term");
        }
        consensusState.applyCatchup(catchUp.getFullState());
        assert sender.equals(localNode) == false;
        assert mode != Mode.LEADER;

        if (storedPublishRequest.isPresent() && matchesNextSlot(storedPublishRequest.get())) {
            logger.debug("handleCatchUp: replaying stored {}", storedPublishRequest.get());
            // TODO wrong sender on the next line, doesn't matter?
            LegislatorPublishResponse publishResponse = handlePublishRequest(sender, storedPublishRequest.get());
            assert publishResponse.getPublishResponse().isPresent();
            storedPublishRequest = Optional.empty();
            return publishResponse.getPublishResponse().get();
        } else {
            throw new ConsensusMessageRejectedException("publish state not stored");
        }
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
        sendStartVote(new StartVoteRequest(currentTerm + 1));
    }

    private void sendStartVote(StartVoteRequest startVoteRequest) {
        nodeSupplier.get().forEach(n -> transport.sendStartVote(n, startVoteRequest, new TransportResponseHandler<Vote>() {
            @Override
            public Vote read(StreamInput in) throws IOException {
                return new Vote(in);
            }

            @Override
            public void handleResponse(Vote response) {
                handleVote(n, response);
            }

            @Override
            public void handleException(TransportException exp) {
                if (exp.getRootCause() instanceof ConsensusMessageRejectedException) {
                    logger.debug("handleVoteResponse: [{}] failed: {}", n, exp.getRootCause().getMessage());
                } else {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "handleVoteResponse: failed to get vote from [{}]", n), exp);
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
            assert consensusState.canHandleClientValue() == false; // follows from electionWon == false, but explicitly stated here again
            assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(localNode) == false);
        }
        //assert currentHeartbeatCollector.isPresent() == (mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_IN_PROGRESS);

        assert (seekVotesScheduler.isPresent()) == (mode == Mode.CANDIDATE);
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    public interface Transport<T> {
        void sendPublishRequest(DiscoveryNode destination, PublishRequest<T> publishRequest,
                                TransportResponseHandler<LegislatorPublishResponse> responseHandler);

        void sendCatchUp(DiscoveryNode destination, CatchupRequest<T> catchUp, TransportResponseHandler<PublishResponse> responseHandler);

        void sendHeartbeatRequest(DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                  TransportResponseHandler<HeartbeatResponse> responseHandler);

        void sendApplyCommit(DiscoveryNode destination, ApplyCommit applyCommit,
                             TransportResponseHandler<TransportResponse.Empty> responseHandler);

        void sendSeekVotes(DiscoveryNode destination, SeekVotes seekVotes, TransportResponseHandler<OfferVote> responseHandler);

        void sendStartVote(DiscoveryNode destination, StartVoteRequest startVoteRequest, TransportResponseHandler<Vote> responseHandler);

        void sendPreVoteHandover(DiscoveryNode destination);

        void sendAbdication(DiscoveryNode destination, long currentTerm);
    }

    public interface FutureExecutor {
        void schedule(TimeValue delay, Runnable task);
    }

    private class SeekVotesScheduler {

        private long currentDelayMillis = 0;
        private boolean running = true;

        SeekVotesScheduler() {
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
            logger.debug("SeekVotesScheduler.handleWakeUp: " +
                    "waking up as {} at [{}] with running={}, slot={}, term={}, lastAcceptedTerm={}",
                mode, currentTimeSupplier.getAsLong(), running,
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());

            if (running) {
                scheduleNextWakeUp();
                startSeekingVotes();
            }
        }
    }

    private class HeartbeatScheduler {

        private boolean running = true;
        private final long term; // for assertions that a new term gets a new scheduler

        HeartbeatScheduler() {
            term = consensusState.getCurrentTerm();
            scheduleNextWakeUp();
        }

        void stop() {
            assert running;
            running = false;
        }

        private void scheduleNextWakeUp() {
            assert running;
            assert mode == Mode.LEADER;
            assert consensusState.getCurrentTerm() == term;
            futureExecutor.schedule(heartbeatDelay, this::handleWakeUp);
        }

        private void handleWakeUp() {
            logger.trace("HeartbeatScheduler.handleWakeUp: " +
                    "waking up as {} at [{}] with running={}, slot={}, term={}, lastAcceptedTerm={}",
                mode, currentTimeSupplier.getAsLong(), running,
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());

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
                    = new HeartbeatRequest(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());

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
                                    logger.trace("Heartbeat.handleResponse: received [{}]", heartbeatResponse);
                                    assert heartbeatResponse.getTerm() <= term;
                                    successfulNodes.add(n);
                                    onPossibleCompletion();
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    logger.debug(
                                        (Supplier<?>) () -> new ParameterizedMessage(
                                            "Heartbeat.handleException: failed to get heartbeat from [{}]", n), exp);
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

                    if (consensusState.isQuorumInCurrentConfiguration(nodeCollection)) {
                        logger.trace("Heartbeat.onPossibleCompletion: received a quorum of responses");
                        receivedQuorum = true;
                        return;
                    }

                    for (DiscoveryNode discoveryNode : allNodes) {
                        if (failedNodes.contains(discoveryNode) == false) {
                            nodeCollection.add(discoveryNode);
                        }
                    }

                    if (consensusState.isQuorumInCurrentConfiguration(nodeCollection) == false) {
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

    private class PassiveFollowerFailureDetector {

        private boolean running = true;

        PassiveFollowerFailureDetector() {
            futureExecutor.schedule(followerTimeout, this::handleWakeUp);
        }

        void stop() {
            assert running;
            running = false;
        }

        private void handleWakeUp() {
            logger.trace("PassiveFollowerFailureDetector.handleWakeUp: " +
                    "waking up as {} at [{}] with running={}, slot={}, term={}, lastAcceptedTerm={}",
                mode, currentTimeSupplier.getAsLong(), running,
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());

            if (running) {
                assert mode == Mode.FOLLOWER;
                becomeCandidate("PassiveFollowerFailureDetector.handleWakeUp");
            }
        }
    }
}

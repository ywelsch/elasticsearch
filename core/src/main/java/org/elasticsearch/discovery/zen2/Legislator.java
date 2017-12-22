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

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.ConsensusState.CommittedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.Vote;

import java.util.Optional;
import java.util.Random;
import java.util.function.LongSupplier;

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
    // the time in leader state without being able to contact a quorum of nodes before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_LEADER_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.leader_timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // the time in follower state without receiving new values or heartbeats from a leader before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_FOLLOWER_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.follower_timeout",
            TimeValue.timeValueMillis(90000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // the delay at which the leader sends out a heartbeat after becoming leader or after a successful publishing/heartbeat round
    public static final Setting<TimeValue> CONSENSUS_HEARTBEAT_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.heartbeat_delay",
            settings -> TimeValue.timeValueMillis((CONSENSUS_LEADER_TIMEOUT_SETTING.get(settings).millis() / 3) + 1),
            TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final ConsensusState<T> consensusState;
    private final Transport<T> transport;
    private final DiscoveryNode localNode;
    private final TimeValue minDelay;
    private final TimeValue maxDelay;
    private final TimeValue leaderTimeout;
    private final TimeValue followerTimeout;
    private final TimeValue heartbeatDelay;
    private final LongSupplier currentTimeSupplier;
    private final Random random;

    private Mode mode;
    private LeaderMode leaderMode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private long currentDelayMillis;
    private long nextWakeUpTimeMillis;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect vote offers.
    private Optional<NodeCollection> votesOffered = Optional.empty();

    private NodeCollection heartbeatNodes;

    // Present if we are catching-up.
    private Optional<PublishRequest<T>> storedPublishRequest = Optional.empty();

    // Present if we are catching-up.
    private Optional<Messages.HeartbeatRequest> storedHeartbeatRequest = Optional.empty();

    // Present if we receive an ApplyCommit while catching up
    private Optional<ApplyCommit> storedApplyCommit = Optional.empty();

    public Legislator(Settings settings, ConsensusState.PersistedState<T> persistedState,
                      Transport<T> transport, DiscoveryNode localNode, LongSupplier currentTimeSupplier) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        leaderTimeout = CONSENSUS_LEADER_TIMEOUT_SETTING.get(settings);
        followerTimeout = CONSENSUS_FOLLOWER_TIMEOUT_SETTING.get(settings);
        heartbeatDelay = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(settings);
        random = Randomness.get();

        consensusState = new ConsensusState<>(settings, persistedState);
        this.transport = transport;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.heartbeatNodes = new NodeCollection();
        lastKnownLeader = Optional.empty();

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

    /**
     * Returns the (non-negative) remaining delay from now when the next relevant wakeup should happen
     */
    public long getNextWakeUpDelayMillis() {
        return getNextWakeUpDelayMillis(currentTimeSupplier.getAsLong());
    }

    private long getNextWakeUpDelayMillis(long now) {
        return Math.max(nextWakeUpTimeMillis - now, 0L);
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private long randomNonNegativeLong() {
        long result = random.nextLong();
        return result == Long.MIN_VALUE ? 0 : Math.abs(result);
    }

    /**
     * @return a random long in [lowerBound, upperBound)
     */
    private long randomLongBetween(long lowerBound, long upperBound) {
        assert 0 < upperBound - lowerBound;
        return randomNonNegativeLong() % (upperBound - lowerBound) + lowerBound;
    }

    private void ignoreWakeUpsForRandomDelay() {
        nextWakeUpTimeMillis = currentTimeSupplier.getAsLong() + randomLongBetween(minDelay.getMillis(), currentDelayMillis + 1);
    }

    public void ignoreWakeUpsForAtLeast(TimeValue delay) {
        final long newWakeUpTimeMillis = currentTimeSupplier.getAsLong() + delay.getMillis();
        if (newWakeUpTimeMillis - nextWakeUpTimeMillis > 0L) {
            nextWakeUpTimeMillis = newWakeUpTimeMillis;
        }
    }

    public void handleWakeUp() {
        long now = currentTimeSupplier.getAsLong();
        final long remainingWakeUpDelay = getNextWakeUpDelayMillis(now);

        try {
            if (remainingWakeUpDelay == 0L) {
                final Level logLevel = mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_DELAY ? Level.TRACE : Level.DEBUG;
                logger.log(logLevel, "handleWakeUp: waking up as [{}] at [{}] with slot={}, term={}, lastAcceptedTerm={}", mode, now,
                    consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
                switch (mode) {
                    case CANDIDATE:
                        currentDelayMillis = Math.min(maxDelay.getMillis(), currentDelayMillis + minDelay.getMillis());
                        ignoreWakeUpsForRandomDelay();
                        startSeekingVotes();
                        break;

                    case LEADER:
                        switch (leaderMode) {
                            case PUBLISH_IN_PROGRESS:
                            case HEARTBEAT_IN_PROGRESS:
                                becomeCandidate("handleWakeUp");
                                break;
                            case HEARTBEAT_DELAY:
                                becomeOrRenewLeader("handleWakeUp", LeaderMode.HEARTBEAT_IN_PROGRESS);
                                transport.broadcastHeartbeat(
                                    new Messages.HeartbeatRequest(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm()));
                                break;
                        }
                        break;

                    case FOLLOWER:
                        becomeCandidate("handleWakeUp");
                        break;
                }
            } else {
                logger.trace("handleWakeUp: ignoring wake-up at [{}], next wake-up delay is [{}]", now,
                    TimeValue.timeValueMillis(getNextWakeUpDelayMillis(now)));
            }
        } finally {
            assert getNextWakeUpDelayMillis(now) > 0L;
        }
    }

    private void becomeCandidate(String method) {
        logger.debug("{}: becoming candidate (lastKnownLeader was [{}])", method, lastKnownLeader);
        mode = Mode.CANDIDATE;
        currentDelayMillis = minDelay.getMillis();
        ignoreWakeUpsForRandomDelay();
    }

    private void becomeOrRenewLeader(String method, LeaderMode newLeaderMode) {
        if (mode != Mode.LEADER) {
            logger.debug("{}: becoming leader [{}] (lastKnownLeader was [{}])", method, newLeaderMode, lastKnownLeader);
        } else {
            assert newLeaderMode != leaderMode;
            // publishing always followed by delaying the heartbeat
            assert leaderMode != LeaderMode.PUBLISH_IN_PROGRESS || newLeaderMode == LeaderMode.HEARTBEAT_DELAY;
            logger.trace("{}: switching leader mode (prev: [{}], now: [{}])", method, leaderMode, newLeaderMode);
        }
        mode = Mode.LEADER;
        leaderMode = newLeaderMode;
        switch (newLeaderMode) {
            case PUBLISH_IN_PROGRESS:
            case HEARTBEAT_IN_PROGRESS:
                ignoreWakeUpsForAtLeast(leaderTimeout);
                break;
            case HEARTBEAT_DELAY:
                ignoreWakeUpsForAtLeast(heartbeatDelay);
                break;
        }
        lastKnownLeader = Optional.of(localNode);
        heartbeatNodes = new NodeCollection();
    }

    private void becomeOrRenewFollower(DiscoveryNode leaderNode) {
        if (mode != Mode.FOLLOWER) {
            logger.debug("handleApplyCommit: becoming follower of [{}] (lastKnownLeader was [{}])",
                leaderNode, lastKnownLeader);
        }
        mode = Mode.FOLLOWER;
        lastKnownLeader = Optional.of(leaderNode);
        ignoreWakeUpsForAtLeast(followerTimeout);
    }

    private void startSeekingVotes() {
        votesOffered = Optional.of(new NodeCollection());
        SeekVotes seekVotes = new SeekVotes(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
        transport.broadcastSeekVotes(seekVotes);
    }

    public void handleStartVote(DiscoveryNode sourceNode, long targetTerm) {
        logger.debug("handleStartVote: from [{}] with term {}", sourceNode, targetTerm);
        Vote vote = consensusState.handleStartVote(targetTerm);
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("handleStartVote");
        }
        transport.sendVote(sourceNode, vote);
    }

    private void ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            handleStartVote(sourceNode, targetTerm);
        }
    }

    public void handleClientValue(Diff<T> diff) {
        if (mode != Mode.LEADER) {
            throw new IllegalArgumentException("handleClientValue: not currently leading, so cannot handle client value.");
        }
        PublishRequest<T> publishRequest = consensusState.handleClientValue(diff);
        becomeOrRenewLeader("handleClientValue", LeaderMode.PUBLISH_IN_PROGRESS);
        transport.broadcastPublishRequest(publishRequest);
    }

    public void handleVote(DiscoveryNode sourceNode, Vote vote) {
        ensureTermAtLeast(localNode, vote.getTerm());

        boolean prevElectionWon = consensusState.electionWon();
        Optional<PublishRequest<T>> maybePublishRequest = consensusState.handleVote(sourceNode, vote);
        assert !prevElectionWon || consensusState.electionWon(); // we cannot go from won to not won
        if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            if (maybePublishRequest.isPresent()) {
                becomeOrRenewLeader("handleVote", LeaderMode.PUBLISH_IN_PROGRESS);
                transport.broadcastPublishRequest(maybePublishRequest.get());
            } else if (consensusState.canHandleClientValue()) {
                becomeOrRenewLeader("handleVote", LeaderMode.HEARTBEAT_IN_PROGRESS);
                transport.broadcastHeartbeat(
                    new Messages.HeartbeatRequest(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm()));
            } else {
                // This is a rare case where a node started an election, then rebooted, and then received enough votes to win the election
                logger.debug("handleVote: election won even though publishing is not permitted");
            }
        }
    }

    public void handlePublishRequest(DiscoveryNode sourceNode, PublishRequest<T> publishRequest) {
        if (publishRequest.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: received request [{}] with future slot (expected [{}]): catching up",
                publishRequest, consensusState.firstUncommittedSlot());
            ensureTermAtLeast(sourceNode, publishRequest.getTerm());
            storedPublishRequest = Optional.of(publishRequest);
            transport.sendRequestCatchUp(sourceNode);
        } else {
            logger.trace("handlePublishRequest: handling [{}] from [{}])", publishRequest, sourceNode);
            ensureTermAtLeast(sourceNode, publishRequest.getTerm());
            PublishResponse publishResponse = consensusState.handlePublishRequest(publishRequest);
            if (sourceNode.equals(localNode) == false) {
                becomeOrRenewFollower(sourceNode);
            }
            transport.sendPublishResponse(sourceNode, publishResponse);
        }
    }

    public void handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, sourceNode);
        assert consensusState.getCurrentTerm() >= publishResponse.getTerm();
        Optional<ApplyCommit> optionalCommit = consensusState.handlePublishResponse(sourceNode, publishResponse);
        // TODO: handle negative votes and move to candidate if leader
        if (optionalCommit.isPresent()) {
            assert mode == Mode.LEADER || mode == Mode.CANDIDATE : localNode.getId() + ": expected leader or candidate but was " + mode;
            transport.broadcastApplyCommit(optionalCommit.get());
        }
    }

    public void handleHeartbeatRequest(DiscoveryNode sourceNode, Messages.HeartbeatRequest heartbeatRequest) {
        if (heartbeatRequest.getSlot() > consensusState.firstUncommittedSlot()) {
            logger.debug("handleHeartbeatRequest: received request [{}] with future slot (expected [{}]): catching up",
                heartbeatRequest, consensusState.firstUncommittedSlot());
            ensureTermAtLeast(sourceNode, heartbeatRequest.getTerm());
            storedHeartbeatRequest = Optional.of(heartbeatRequest);
            transport.sendRequestCatchUp(sourceNode);
        } else {
            logger.trace("handleHeartbeatRequest: handling [{}] from [{}])", heartbeatRequest, sourceNode);
            ensureTermAtLeast(sourceNode, heartbeatRequest.getTerm());
            if (matchesNextSlot(heartbeatRequest)) {
                if (sourceNode.equals(localNode) == false) {
                    becomeOrRenewFollower(sourceNode);
                }
                transport.sendHeartbeatResponse(sourceNode,
                    new Messages.HeartbeatResponse(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm()));
            }
        }
    }

    public void handleHeartbeatResponse(DiscoveryNode sourceNode, Messages.HeartbeatResponse heartbeatResponse) {
        if (mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_IN_PROGRESS) {
            if (consensusState.firstUncommittedSlot() == heartbeatResponse.getSlot() &&
                consensusState.getCurrentTerm() == heartbeatResponse.getTerm()) {
                heartbeatNodes.add(sourceNode);
                if (consensusState.isQuorumInCurrentConfiguration(heartbeatNodes)) {
                    logger.trace("handleHeartbeatResponse: renewing leader lease");
                    becomeOrRenewLeader("handleHeartbeatResponse", LeaderMode.HEARTBEAT_DELAY);
                    heartbeatNodes = new NodeCollection(); // TODO record all the heartbeat responses
                }
            }
        }
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit) {

        if (applyCommit.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handleApplyCommit: storing future {}", applyCommit);
            storedApplyCommit = Optional.of(applyCommit);
            return;
        }

        logger.trace("handleApplyCommit: applying {}", applyCommit);

        boolean prevElectionWon = consensusState.electionWon();
        consensusState.handleCommit(applyCommit);
        if (prevElectionWon && consensusState.electionWon() && mode == Mode.LEADER) {
            logger.trace("handleApplyCommit: renewing leader lease");
            assert consensusState.canHandleClientValue();
            becomeOrRenewLeader("handleApplyCommit", LeaderMode.HEARTBEAT_DELAY);
        } else if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode != Mode.LEADER : "expected non-leader but was leader";
            assert consensusState.canHandleClientValue();
            becomeOrRenewLeader("handleApplyCommit", LeaderMode.HEARTBEAT_IN_PROGRESS);
            transport.broadcastHeartbeat(
                new Messages.HeartbeatRequest(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm()));
        } else if (prevElectionWon && consensusState.electionWon() == false) {
            assert mode == Mode.LEADER || mode == Mode.CANDIDATE : localNode.getId() + ": expected leader or candidate but was " + mode;
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("handleApplyCommit");
            }
        }

        storedPublishRequest = Optional.empty();
        storedApplyCommit = Optional.empty();
    }

    public void handleSeekVotes(DiscoveryNode sender, Messages.SeekVotes seekVotes) {
        logger.debug("handleSeekVotes: received [{}] from [{}]", seekVotes, sender);

        if (mode == Mode.CANDIDATE) {
            Messages.OfferVote offerVote = new Messages.OfferVote(consensusState.firstUncommittedSlot(),
                consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
            logger.debug("handleSeekVotes: candidate received [{}] from [{}] and responding with [{}]", seekVotes, sender, offerVote);
            transport.sendOfferVote(sender, offerVote);
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
            transport.broadcastStartVote(newTerm);
        } else {
            logger.debug("handleSeekVotes: not offering vote: slot={}, term={}, mode={}",
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), mode);
        }
    }

    public void handleOfferVote(DiscoveryNode sender, Messages.OfferVote offerVote) {
        if (votesOffered.isPresent() == false) {
            logger.debug("handleOfferVote: received OfferVote message from [{}] but not collecting offers.", sender);
            throw new IllegalArgumentException("Received OfferVote but not collecting offers.");
        }

        if (offerVote.getFirstUncommittedSlot() > consensusState.firstUncommittedSlot()
            || (offerVote.getFirstUncommittedSlot() == consensusState.firstUncommittedSlot()
            && offerVote.getLastAcceptedTerm() > consensusState.lastAcceptedTerm())) {
            logger.debug("handleOfferVote: handing over pre-voting to [{}] because of {}", sender, offerVote);
            votesOffered = Optional.empty();
            transport.sendPreVoteHandover(sender);
            return;
        }

        logger.debug("handleOfferVote: received {} from [{}]", offerVote, sender);
        ensureTermAtLeast(localNode, offerVote.getTerm());
        votesOffered.get().add(sender);

        if (consensusState.isQuorumInCurrentConfiguration(votesOffered.get())) {
            logger.debug("handleOfferVote: received a quorum of OfferVote messages, so starting an election.");
            votesOffered = Optional.empty();
            transport.broadcastStartVote(consensusState.getCurrentTerm() + 1);
        }
    }

    public void handlePreVoteHandover(DiscoveryNode sender) {
        logger.debug("handlePreVoteHandover: received handover from [{}]", sender);
        startSeekingVotes();
    }

    public void handleRequestCatchUp(DiscoveryNode sender) {
        assert sender.equals(localNode) == false;
        T committedState = consensusState.generateCatchup();
        logger.debug("handleRequestCatchUp: sending catch-up to {} with slot {}", sender, committedState.getSlot());
        transport.sendCatchUp(sender, committedState);
    }

    public boolean matchesNextSlot(Messages.SlotTerm slotTerm) {
        return slotTerm.getTerm() == consensusState.getCurrentTerm() &&
            slotTerm.getSlot() == consensusState.firstUncommittedSlot();
    }

    public void handleCatchUp(DiscoveryNode sender, T catchUp) {
        logger.debug("handleCatchUp: received catch-up from {} to slot {}", sender, catchUp.getSlot());
        consensusState.applyCatchup(catchUp);
        if (mode == Mode.LEADER) {
            logger.debug("stale leader receiving catch-up from {} to slot {}", sender, catchUp.getSlot());
            becomeCandidate("handleCatchup");
        }

        if (storedHeartbeatRequest.isPresent()) {
            logger.debug("handleCatchUp: replaying stored {}", storedHeartbeatRequest.get());
            if (matchesNextSlot(storedHeartbeatRequest.get())) {
                handleHeartbeatRequest(sender, storedHeartbeatRequest.get()); // TODO wrong sender, doesn't matter?
                storedHeartbeatRequest = Optional.empty();
            }
        }
        if (storedPublishRequest.isPresent()) {
            logger.debug("handleCatchUp: replaying stored {}", storedPublishRequest.get());
            if (matchesNextSlot(storedPublishRequest.get())) {
                handlePublishRequest(sender, storedPublishRequest.get()); // TODO wrong sender, doesn't matter?
                storedPublishRequest = Optional.empty();
            }
        }
        if (storedApplyCommit.isPresent()) {
            logger.debug("handleCatchUp: replaying stored {}", storedApplyCommit.get());
            handleApplyCommit(sender, storedApplyCommit.get()); // TODO wrong sender, doesn't matter?
            storedApplyCommit = Optional.empty();
        }
    }

    public void abdicateTo(DiscoveryNode newLeader) {
        if (mode != Mode.LEADER) {
            logger.debug("abdicateTo: mode={} so cannot abdicate to [{}]", mode, newLeader);
            throw new IllegalArgumentException("abdicateTo: not currently leading, so cannot abdicate.");
        }
        logger.debug("abdicateTo: abdicating to [{}]", newLeader);
        transport.sendAbdication(newLeader, consensusState.getCurrentTerm());
    }

    public void handleAbdication(DiscoveryNode sender, long currentTerm) {
        logger.debug("handleAbdication: accepting abdication from [{}] in term {}", sender, currentTerm);
        transport.broadcastStartVote(currentTerm + 1);
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
            assert (leaderMode != LeaderMode.PUBLISH_IN_PROGRESS) == consensusState.canHandleClientValue();
            assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(localNode);
        } else if (mode == Mode.FOLLOWER) {
            assert consensusState.electionWon() == false;
            assert consensusState.canHandleClientValue() == false; // follows from electionWon == false, but explicitly stated here again
            assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(localNode) == false);
        }
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    public enum LeaderMode {
        PUBLISH_IN_PROGRESS, HEARTBEAT_DELAY, HEARTBEAT_IN_PROGRESS
    }

    public interface Transport<T> {
        void sendVote(DiscoveryNode destination, Vote vote);

        void broadcastPublishRequest(PublishRequest<T> publishRequest);

        void sendPublishResponse(DiscoveryNode destination, PublishResponse publishResponse);

        void broadcastHeartbeat(Messages.HeartbeatRequest heartbeatRequest);

        void sendHeartbeatResponse(DiscoveryNode destination, Messages.HeartbeatResponse heartbeatResponse);

        void broadcastApplyCommit(ApplyCommit applyCommit);

        void broadcastSeekVotes(Messages.SeekVotes seekVotes);

        void sendOfferVote(DiscoveryNode destination, Messages.OfferVote offerVote);

        void broadcastStartVote(long term);

        void sendPreVoteHandover(DiscoveryNode destination);

        void sendRequestCatchUp(DiscoveryNode destination);

        void sendCatchUp(DiscoveryNode destination, T catchUp);

        void sendAbdication(DiscoveryNode destination, long currentTerm);
    }
}

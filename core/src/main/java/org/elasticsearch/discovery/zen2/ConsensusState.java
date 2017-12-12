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

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SlotTermDiff;
import org.elasticsearch.discovery.zen2.Messages.Vote;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.discovery.zen2.Messages.NO_TERM;

/**
 * The safety core of the consensus algorithm, which ensures that any two instances
 * of this object anywhere in the cluster have the same committedState if they are
 * at the same slot.
 *
 * @param <T> The state on which we achieve consensus.
 */
public class ConsensusState<T extends ConsensusState.CommittedState> extends AbstractComponent {

    final Persistence<T> persistence;
    // persisted state
    private long currentTerm;
    private T committedState;
    private Optional<SlotTermDiff<T>> acceptedState;
    // transient state
    private boolean electionWon;
    private boolean electionValueForced;
    private NodeCollection joinVotes;
    private boolean publishPermitted;
    private NodeCollection publishVotes;

    public ConsensusState(Settings settings, long currentTerm, T committedState, Optional<SlotTermDiff<T>> acceptedState,
                          Persistence<T> persistence) {
        // TODO idea: just pass in a Persistence and let it provide the persisted state.

        super(settings);
        this.persistence = persistence;

        // persisted state
        this.currentTerm = currentTerm;
        this.committedState = committedState;
        this.acceptedState = acceptedState;

        // transient state
        this.electionWon = false;
        this.electionValueForced = false;
        this.joinVotes = new NodeCollection();
        this.publishPermitted = false;
        this.publishVotes = new NodeCollection();

        assert currentTerm >= 0;
        assert acceptedState.isPresent() == false || acceptedState.get().getTerm() <= currentTerm;
        assert acceptedState.isPresent() == false || acceptedState.get().getSlot() <= firstUncommittedSlot();
    }

    public T getCommittedState() {
        return committedState;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public boolean isQuorumInCurrentConfiguration(NodeCollection votes) {
        final HashSet<String> intersection = new HashSet<>(committedState.getVotingNodes().nodes.keySet());
        intersection.retainAll(votes.nodes.keySet());
        return intersection.size() * 2 > committedState.getVotingNodes().nodes.size();
    }

    public long firstUncommittedSlot() {
        return committedState.getSlot() + 1;
    }

    public long lastAcceptedTermInSlot() {
        if (acceptedState.isPresent() && firstUncommittedSlot() == acceptedState.get().getSlot()) {
            return acceptedState.get().getTerm();
        } else {
            return NO_TERM;
        }
    }

    /**
     * May be safely called at any time to move this instance to a new term. It is vitally important for safety that
     * the resulting Vote is sent to no more than one node.
     *
     * @param newTerm The new term
     * @return A Vote that must be sent to at most one other node.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Vote handleStartVote(long newTerm) {
        if (newTerm <= currentTerm) {
            logger.debug("handleStartVote: ignored as term provided [{}] lower or equal than current term [{}]",
                newTerm, currentTerm);
            throw new IllegalArgumentException("incoming term " + newTerm + " lower than current term " + currentTerm);
        }

        logger.debug("handleStartVote: updating term from [{}] to [{}]", currentTerm, newTerm);

        persistence.persistCurrentTerm(newTerm);
        currentTerm = newTerm;
        joinVotes = new NodeCollection();
        electionWon = false;
        electionValueForced = false;
        publishPermitted = true;
        publishVotes = new NodeCollection();

        return new Vote(firstUncommittedSlot(), currentTerm, lastAcceptedTermInSlot());
    }

    /**
     * May be called on receipt of a Vote from the given sourceNode.
     *
     * @param sourceNode The sender of the Vote received.
     * @param vote       The Vote received.
     * @return An optional PublishRequest which, if present, can be broadcast to all peers.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Optional<PublishRequest<T>> handleVote(DiscoveryNode sourceNode, Vote vote) {
        if (vote.getTerm() != currentTerm) {
            logger.debug("handleVote: ignored vote due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, vote.getTerm());
            throw new IllegalArgumentException("incoming term " + vote.getTerm() + " does not match current term " + currentTerm);
        }
        if (vote.getFirstUncommittedSlot() > firstUncommittedSlot()) {
            logger.debug("handleVote: ignored vote due to slot mismatch (expected: <=[{}], actual: [{}])",
                firstUncommittedSlot(), vote.getFirstUncommittedSlot());
            throw new IllegalArgumentException("incoming slot " + vote.getFirstUncommittedSlot() + " higher than current slot " +
                firstUncommittedSlot());
        }
        if (vote.getFirstUncommittedSlot() == firstUncommittedSlot() && vote.getLastAcceptedTerm() != NO_TERM) {
            final long lastAcceptedTermInSlot = lastAcceptedTermInSlot();
            if (vote.getLastAcceptedTerm() > lastAcceptedTermInSlot) {
                logger.debug("handleVote: ignored vote as voter has better last accepted term (expected: <=[{}], actual: [{}])",
                    lastAcceptedTermInSlot, vote.getLastAcceptedTerm());
                throw new IllegalArgumentException("incoming last accepted term " + vote.getLastAcceptedTerm() + " higher than " +
                    "current last accepted term " + lastAcceptedTermInSlot);
            }
            if (vote.getLastAcceptedTerm() < lastAcceptedTermInSlot && electionValueForced == false) {
                logger.debug("handleVote: ignored vote as voter has worse last accepted term and election value not forced " +
                    "(expected: <=[{}], actual: [{}])", lastAcceptedTermInSlot, vote.getLastAcceptedTerm());
                throw new IllegalArgumentException("incoming last accepted term " + vote.getLastAcceptedTerm() + " lower than " +
                    "current last accepted term " + lastAcceptedTermInSlot + " and election value not forced");
            }
            electionValueForced = true;
        }

        logger.debug("handleVote: adding vote {} from [{}] for election at slot {}", vote, sourceNode.getId(), firstUncommittedSlot());
        joinVotes.add(sourceNode);

        electionWon = isQuorumInCurrentConfiguration(joinVotes);

        logger.debug("handleVote: electionWon={} publishPermitted={} electionValueForce={}",
            electionWon, publishPermitted, electionValueForced);
        if (electionWon && publishPermitted && electionValueForced) {
            logger.debug("handleVote: sending PublishRequest");

            publishPermitted = false;
            assert acceptedState.isPresent(); // must be true because electionValueForced == true
            return Optional.of(new PublishRequest<>(firstUncommittedSlot(), currentTerm, acceptedState.get().getDiff()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of a PublishRequest.
     *
     * @param publishRequest The PublishRequest received.
     * @return A PublishResponse which can be sent back to the sender of the PublishRequest.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public PublishResponse handlePublishRequest(PublishRequest<T> publishRequest) {
        if (publishRequest.getTerm() != currentTerm) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, publishRequest.getTerm());
            throw new IllegalArgumentException("incoming term " + publishRequest.getTerm() + " does not match current term " +
                currentTerm);
        }
        if (publishRequest.getSlot() != firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: ignored publish request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), publishRequest.getSlot());
            throw new IllegalArgumentException("incoming slot " + publishRequest.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishRequest: storing publish request for slot [{}] and term [{}]",
            publishRequest.getSlot(), publishRequest.getTerm());
        persistence.persistAcceptedState(publishRequest);
        acceptedState = Optional.of(publishRequest);

        return new PublishResponse(publishRequest.getSlot(), publishRequest.getTerm());
    }

    /**
     * May be called on receipt of a PublishResponse from the given sourceNode.
     *
     * @param sourceNode      The sender of the PublishResponse received.
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommit which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Optional<ApplyCommit> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        if (publishResponse.getTerm() != currentTerm) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, publishResponse.getTerm());
            throw new IllegalArgumentException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + currentTerm);
        }
        if (publishResponse.getSlot() != firstUncommittedSlot()) {
            if (publishResponse.getSlot() == firstUncommittedSlot() - 1) {
                logger.trace("handlePublishResponse: ignored publish response for just-committed slot [{}]", publishResponse.getSlot());
            } else {
                logger.debug("handlePublishResponse: ignored publish response due to slot mismatch (expected: [{}], actual: [{}])",
                    firstUncommittedSlot(), publishResponse.getSlot());
            }
            throw new IllegalArgumentException("incoming slot " + publishResponse.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishResponse: accepted publish response for slot [{}] and term [{}] from [{}]",
            publishResponse.getSlot(), publishResponse.getTerm(), sourceNode.getId());
        publishVotes.add(sourceNode);
        if (isQuorumInCurrentConfiguration(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for slot [{}] and term [{}]",
                firstUncommittedSlot(), currentTerm);
            return Optional.of(new ApplyCommit(publishResponse.getSlot(), publishResponse.getTerm()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of an ApplyCommit. Updates the committed state accordingly.
     *
     * @param applyCommit The ApplyCommit received.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public void handleCommit(ApplyCommit applyCommit) {
        if (applyCommit.getTerm() != lastAcceptedTermInSlot()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch (expected: [{}], actual: [{}])",
                lastAcceptedTermInSlot(), applyCommit.getTerm());
            throw new IllegalArgumentException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                lastAcceptedTermInSlot());
        }
        if (applyCommit.getSlot() != firstUncommittedSlot()) {
            logger.debug("handleCommit: ignored commit request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), applyCommit.getSlot());
            throw new IllegalArgumentException("incoming slot " + applyCommit.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handleCommit: applying commit request for slot [{}]", applyCommit.getSlot());

        assert acceptedState.isPresent();
        assert acceptedState.get().getSlot() == committedState.getSlot() + 1;
        final T newCommittedState = acceptedState.get().getDiff().apply(committedState);
        logger.trace("handleCommit: newCommittedState = [{}]", newCommittedState);
        assert newCommittedState.getSlot() == committedState.getSlot() + 1;

        persistence.persistCommittedState(newCommittedState);
        committedState = newCommittedState;
        publishPermitted = true;
        electionValueForced = false;
        publishVotes = new NodeCollection();
    }

    /**
     * May be safely called at any time. Yields the current committed state to be sent to another, lagging, peer so it can catch up.
     *
     * @return The current committed state.
     */
    public T generateCatchup() {
        logger.debug("generateCatchup: generating catch up for slot [{}]", committedState.getSlot());
        return committedState;
    }

    /**
     * May be called on receipt of a catch-up message containing the current committed state from a peer.
     *
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public void applyCatchup(T newCommittedState) {
        if (newCommittedState.getSlot() <= committedState.getSlot()) {
            logger.debug("applyCatchup: ignored catch up request due to slot mismatch (expected: >[{}], actual: [{}])",
                committedState.getSlot(), newCommittedState.getSlot());
            throw new IllegalArgumentException("incoming slot " + newCommittedState.getSlot() + " higher than current slot " +
                committedState.getSlot());
        }

        logger.debug("applyCatchup: applying catch up for slot [{}]", newCommittedState.getSlot());
        persistence.persistCommittedState(newCommittedState);
        committedState = newCommittedState;
        electionValueForced = false;
        joinVotes = new NodeCollection();
        electionWon = false;
        publishVotes = new NodeCollection();
        publishPermitted = false;
    }

    /**
     * May be safely called at any time in order to apply the given diff to the replicated state machine.
     *
     * @param diff The RSM transition on which to achieve consensus.
     * @return A PublishRequest that may be broadcast to all peers.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public PublishRequest<T> handleClientValue(Diff<T> diff) {
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new IllegalArgumentException("election not won");
        }
        if (publishPermitted == false) {
            logger.debug("handleClientValue: ignored request as publishing is not permitted");
            throw new IllegalArgumentException("publishing not permitted");
        }
        if (electionValueForced) {
            logger.debug("handleClientValue: ignored request as election value is forced");
            throw new IllegalArgumentException("election value forced");
        }

        logger.trace("handleClientValue: processing request for slot [{}] and term [{}]", firstUncommittedSlot(), currentTerm);
        publishPermitted = false;
        return new PublishRequest<>(firstUncommittedSlot(), currentTerm, diff);
    }

    /**
     * An immutable object representing the current committed state.
     */
    public interface CommittedState {
        long getSlot();

        NodeCollection getVotingNodes();
    }

    public interface Persistence<T> {
        void persistCurrentTerm(long currentTerm);

        void persistCommittedState(T committedState);

        void persistAcceptedState(SlotTermDiff<T> slotTermDiff);
    }

    /**
     * A collection of nodes, used to calculate quorums.
     */
    public static class NodeCollection {

        protected final Map<String, DiscoveryNode> nodes = new HashMap<>();

        public void add(DiscoveryNode sourceNode) {
            // TODO is getId() unique enough or is it user-provided? If the latter, there's a risk of duplicates or of losing votes.
            nodes.put(sourceNode.getId(), sourceNode);
        }

        @Override
        public String toString() {
            return "NodeCollection{" + String.join(",", nodes.keySet()) + "}";
        }
    }
}

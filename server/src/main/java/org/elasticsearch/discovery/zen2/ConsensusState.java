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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.Join;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The safety core of the consensus algorithm
 */
public class ConsensusState extends AbstractComponent {

    // persisted state
    final PersistedState persistedState;

    // transient state
    private NodeCollection joinVotes;
    private boolean startedJoinSinceLastReboot;
    private boolean electionWon;
    private long lastPublishedVersion;
    private VotingConfiguration lastPublishedConfiguration;
    private NodeCollection publishVotes;

    public ConsensusState(Settings settings, PersistedState persistedState) {
        super(settings);

        // persisted state
        this.persistedState = persistedState;

        // transient state
        this.electionWon = false;
        this.joinVotes = new NodeCollection();
        this.startedJoinSinceLastReboot = false;
        this.publishVotes = new NodeCollection();
        this.lastPublishedVersion = 0L;
        this.lastPublishedConfiguration = persistedState.getLastAcceptedConfiguration();
    }

    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }

    public boolean isElectionQuorum(NodeCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(getLastAcceptedConfiguration());
    }

    public boolean isPublishQuorum(NodeCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(lastPublishedConfiguration);
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return persistedState.getLastCommittedConfiguration();
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return persistedState.getLastAcceptedConfiguration();
    }

    public long getLastAcceptedVersion() {
        return persistedState.getLastAcceptedVersion();
    }

    public long getLastAcceptedTerm() {
        return persistedState.getLastAcceptedTerm();
    }

    public boolean electionWon() {
        return electionWon;
    }

    public long getLastPublishedVersion() {
        return lastPublishedVersion;
    }

    /**
     * May be safely called at any time to move this instance to a new term. It is vitally important for safety that
     * the resulting Join is sent to no more than one node.
     *
     * @param targetNode The node to join
     * @param newTerm The new term
     * @return A Join that must be sent to at most one other node.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Join handleStartJoin(DiscoveryNode targetNode, long newTerm) {
        if (newTerm <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignored as term provided [{}] lower or equal than current term [{}]",
                newTerm, getCurrentTerm());
            throw new ConsensusMessageRejectedException("incoming term " + newTerm + " lower than current term " + getCurrentTerm());
        }

        logger.debug("handleStartJoin: updating term from [{}] to [{}]", getCurrentTerm(), newTerm);

        persistedState.setCurrentTerm(newTerm);
        assert persistedState.getCurrentTerm() == newTerm;
        joinVotes = new NodeCollection();
        electionWon = false;
        startedJoinSinceLastReboot = true;
        lastPublishedVersion = 0L;
        lastPublishedConfiguration = persistedState.getLastAcceptedConfiguration();
        publishVotes = new NodeCollection();

        return new Join(targetNode, getLastAcceptedVersion(), getCurrentTerm(), getLastAcceptedTerm());
    }

    /**
     * May be called on receipt of a Join from the given sourceNode.
     *
     * @param sourceNode The sender of the Join received.
     * @param join       The Join received.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public void handleJoin(DiscoveryNode sourceNode, Join join) {
        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new ConsensusMessageRejectedException("ignored join as term was not incremented yet after reboot");
        }

        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), join.getTerm());
            throw new ConsensusMessageRejectedException(
                "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }

        final long lastAcceptedTerm = getLastAcceptedTerm();
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new ConsensusMessageRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                " of join higher than current last accepted term " + lastAcceptedTerm);
        }

        if (join.getLastAcceptedTerm() == lastAcceptedTerm && join.getLastAcceptedVersion() > getLastAcceptedVersion()) {
            logger.debug("handleJoin: ignored join due to version mismatch (expected: <=[{}], actual: [{}])",
                getLastAcceptedVersion(), join.getLastAcceptedVersion());
            throw new ConsensusMessageRejectedException(
                "incoming version " + join.getLastAcceptedVersion() + " of join higher than current version " + getLastAcceptedVersion());
        }

        joinVotes.add(sourceNode);
        boolean prevElectionWon = electionWon;
        electionWon = isElectionQuorum(joinVotes);
        logger.debug("handleJoin: added join {} from [{}] for election, electionWon={} lastAcceptedTerm={} lastAcceptedVersion={}", join,
            sourceNode, electionWon, lastAcceptedTerm, getLastAcceptedVersion());

        if (electionWon && prevElectionWon == false) {
            lastPublishedVersion = getLastAcceptedVersion();
        }
    }

    /**
     * May be called in order to check if the given cluster state can be published
     *
     * @param clusterState The cluster state which to publish.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public PublishRequest handleClientValue(ClusterState clusterState) {
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new ConsensusMessageRejectedException("election not won");
        }
        if (lastPublishedVersion != getLastAcceptedVersion()) {
            logger.debug("handleClientValue: cannot start publishing next value before accepting previous one");
            throw new ConsensusMessageRejectedException("cannot start publishing next value before accepting previous one");
        }
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handleClientValue: ignored request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new ConsensusMessageRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        if (clusterState.version() <= lastPublishedVersion) {
            logger.debug("handleClientValue: ignored request due to version mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new ConsensusMessageRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }

        if (clusterState.getLastAcceptedConfiguration().equals(getLastAcceptedConfiguration()) == false
            && getLastCommittedConfiguration().equals(getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration while not already reconfiguring");
            throw new ConsensusMessageRejectedException("only allow reconfiguration while not already reconfiguring");
        }
        if (clusterState.getLastAcceptedConfiguration().hasQuorum(joinVotes.nodes.keySet()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration if join quorum available for new config");
            throw new ConsensusMessageRejectedException("only allow reconfiguration if join quorum available for new config");
        }

        lastPublishedVersion = clusterState.version();
        lastPublishedConfiguration = clusterState.getLastAcceptedConfiguration();
        publishVotes = new NodeCollection();

        logger.trace("handleClientValue: processing request for version [{}] and term [{}]", lastPublishedVersion, getCurrentTerm());

        return new PublishRequest(clusterState);
    }

    /**
     * May be called on receipt of a PublishRequest.
     *
     * @param publishRequest The publish request received.
     * @return A PublishResponse which can be sent back to the sender of the PublishRequest.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public PublishResponse handlePublishRequest(PublishRequest publishRequest) {
        final ClusterState clusterState = publishRequest.getAcceptedState();
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), clusterState.term());
            throw new ConsensusMessageRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        if (clusterState.term() == getLastAcceptedTerm() && clusterState.version() <= getLastAcceptedVersion()) {
            logger.debug("handlePublishRequest: ignored publish request due to version mismatch (expected: >[{}], actual: [{}])",
                getLastAcceptedVersion(), clusterState.version());
            throw new ConsensusMessageRejectedException("incoming version " + clusterState.version() + " older than current version " +
                getLastAcceptedVersion());
        }

        logger.trace("handlePublishRequest: accepting publish request for version [{}] and term [{}]",
            clusterState.version(), clusterState.term());
        persistedState.setLastAcceptedState(clusterState);
        assert persistedState.getLastAcceptedState() == clusterState;

        return new PublishResponse(clusterState.version(), clusterState.term());
    }

    /**
     * May be called on receipt of a PublishResponse from the given sourceNode.
     *
     * @param sourceNode      The sender of the PublishResponse received.
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommit which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Optional<ApplyCommit> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        if (electionWon == false) {
            logger.debug("handlePublishResponse: ignored response as election not won");
            throw new ConsensusMessageRejectedException("election not won");
        }
        if (publishResponse.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishResponse.getTerm());
            throw new ConsensusMessageRejectedException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + getCurrentTerm());
        }
        if (publishResponse.getVersion() != lastPublishedVersion) {
            logger.debug("handlePublishResponse: ignored publish response due to version mismatch (expected: [{}], actual: [{}])",
                lastPublishedVersion, publishResponse.getVersion());
            throw new ConsensusMessageRejectedException("incoming version " + publishResponse.getVersion() +
                " does not match current version " + lastPublishedVersion);
        }

        logger.trace("handlePublishResponse: accepted publish response for version [{}] and term [{}] from [{}]",
            publishResponse.getVersion(), publishResponse.getTerm(), sourceNode);
        publishVotes.add(sourceNode);
        if (isPublishQuorum(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for version [{}] and term [{}]",
                publishResponse.getVersion(), publishResponse.getTerm());
            return Optional.of(new ApplyCommit(publishResponse.getTerm(), publishResponse.getVersion()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of an ApplyCommit. Updates the committed state accordingly.
     *
     * @param applyCommit The ApplyCommit received.
     * @throws ConsensusMessageRejectedException if the arguments were incompatible with the current state of this object.
     */
    public void handleCommit(ApplyCommit applyCommit) {
        if (applyCommit.getTerm() != getCurrentTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new ConsensusMessageRejectedException("incoming term " + applyCommit.getTerm() + " does not match current term " +
                getCurrentTerm());
        }
        if (applyCommit.getTerm() != getLastAcceptedTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new ConsensusMessageRejectedException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                getLastAcceptedTerm());
        }
        if (applyCommit.getVersion() != getLastAcceptedVersion()) {
            logger.debug("handleCommit: ignored commit request due to version mismatch (term {}, expected: [{}], actual: [{}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getVersion());
            throw new ConsensusMessageRejectedException("incoming version " + applyCommit.getVersion() +
                " does not match current version " + getLastAcceptedVersion());
        }

        logger.trace("handleCommit: applying commit request for term [{}] and version [{}]", applyCommit.getTerm(),
            applyCommit.getVersion());

        persistedState.markLastAcceptedConfigAsCommitted();
        assert getLastCommittedConfiguration().equals(getLastAcceptedConfiguration());
    }

    public interface PersistedState {

        void setCurrentTerm(long currentTerm);

        void setLastAcceptedState(ClusterState clusterState);

        void markLastAcceptedConfigAsCommitted();

        long getCurrentTerm();

        ClusterState getLastAcceptedState();

        default long getLastAcceptedVersion() {
            return getLastAcceptedState().version();
        }

        default long getLastAcceptedTerm() {
            return getLastAcceptedState().term();
        }

        default VotingConfiguration getLastAcceptedConfiguration() {
            return getLastAcceptedState().getLastAcceptedConfiguration();
        }

        default VotingConfiguration getLastCommittedConfiguration() {
            return getLastAcceptedState().getLastCommittedConfiguration();
        }
    }

    public static class BasePersistedState implements PersistedState {

        private long currentTerm;
        private ClusterState acceptedState;

        public BasePersistedState(long term, ClusterState acceptedState) {
            this.currentTerm = term;
            this.acceptedState = acceptedState;

            assert currentTerm >= 0;
            assert getLastAcceptedTerm() <= currentTerm :
                "last accepted term " + getLastAcceptedTerm() + " cannot be above current term " + currentTerm;
        }

        // copy constructor
        public BasePersistedState(PersistedState persistedState) {
            this.currentTerm = persistedState.getCurrentTerm();
            this.acceptedState = persistedState.getLastAcceptedState();
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            assert this.currentTerm <= currentTerm;
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            this.acceptedState = clusterState;
        }

        @Override
        public void markLastAcceptedConfigAsCommitted() {
            this.acceptedState = ClusterState.builder(acceptedState)
                .lastCommittedConfiguration(acceptedState.getLastAcceptedConfiguration())
                .build();
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return acceptedState;
        }

    }

    /**
     * A collection of nodes, used to calculate quorums.
     */
    public static class NodeCollection {

        private final Map<String, DiscoveryNode> nodes;

        public void add(DiscoveryNode sourceNode) {
            // TODO is getId() unique enough or is it user-provided? If the latter, there's a risk of duplicates or of losing votes.
            nodes.put(sourceNode.getId(), sourceNode);
        }

        public NodeCollection() {
            nodes = new HashMap<>();
        }

        public boolean isQuorum(VotingConfiguration configuration) {
            return configuration.hasQuorum(nodes.keySet());
        }

        @Override
        public String toString() {
            return "NodeCollection{" + String.join(",", nodes.keySet()) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodeCollection that = (NodeCollection) o;

            return nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            return nodes.hashCode();
        }

    }
}

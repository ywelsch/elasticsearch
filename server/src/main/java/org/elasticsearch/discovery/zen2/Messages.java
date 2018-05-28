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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Optional;

public class Messages {

    public static final long NO_TERM = -1L;

    public static class OfferJoin extends TransportResponse {
        protected final long lastAcceptedVersion;
        protected final long term;
        protected final long lastAcceptedTerm;

        public OfferJoin(long lastAcceptedVersion, long term, long lastAcceptedTerm) {
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.term = term;
            this.lastAcceptedTerm = lastAcceptedTerm;
        }

        public OfferJoin(StreamInput in) throws IOException {
            lastAcceptedVersion = in.readLong();
            term = in.readLong();
            lastAcceptedTerm = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(lastAcceptedVersion);
            out.writeLong(term);
            out.writeLong(lastAcceptedTerm);
        }

        public long getLastAcceptedVersion() {
            return lastAcceptedVersion;
        }

        public long getTerm() {
            return term;
        }

        public long getLastAcceptedTerm() {
            return lastAcceptedTerm;
        }

        @Override
        public String toString() {
            return "OfferJoin{lastAcceptedVersion=" + lastAcceptedVersion
                + ", term=" + term + ", lastAcceptedTerm=" + lastAcceptedTerm + '}';
        }
    }

    public static class SeekJoins extends TermVersion {
        public SeekJoins(DiscoveryNode sourceNode, long term, long version) {
            super(sourceNode, term, version);
        }

        public SeekJoins(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "SeekJoins{" + "term=" + term + ", version=" + version + '}';
        }
    }

    public static class Join extends TransportRequest {
        private final DiscoveryNode sourceNode;
        private final DiscoveryNode targetNode;
        private final long term;
        private final long lastAcceptedVersion;
        private final long lastAcceptedTerm;

        public Join(DiscoveryNode sourceNode, DiscoveryNode targetNode, long lastAcceptedVersion, long term, long lastAcceptedTerm) {
            assert term >= 0;
            assert lastAcceptedVersion >= 0;
            assert lastAcceptedTerm >= 0;

            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
            this.term = term;
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.lastAcceptedTerm = lastAcceptedTerm;
        }

        public Join(StreamInput in) throws IOException {
            super(in);
            sourceNode = new DiscoveryNode(in);
            targetNode = new DiscoveryNode(in);
            term = in.readLong();
            lastAcceptedVersion = in.readLong();
            lastAcceptedTerm = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            targetNode.writeTo(out);
            out.writeLong(term);
            out.writeLong(lastAcceptedVersion);
            out.writeLong(lastAcceptedTerm);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

        public DiscoveryNode getTargetNode() {
            return targetNode;
        }

        public long getLastAcceptedVersion() {
            return lastAcceptedVersion;
        }

        public long getTerm() {
            return term;
        }

        public long getLastAcceptedTerm() {
            return lastAcceptedTerm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Join join = (Join) o;

            if (sourceNode.equals(join.sourceNode) == false) return false;
            if (targetNode.equals(join.targetNode) == false) return false;
            if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
            if (term != join.term) return false;
            return lastAcceptedTerm == join.lastAcceptedTerm;
        }

        @Override
        public int hashCode() {
            int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
            result = 31 * result + sourceNode.hashCode();
            result = 31 * result + targetNode.hashCode();
            result = 31 * result + (int) (term ^ (term >>> 32));
            result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Join{" +
                "term=" + term +
                ", lastAcceptedVersion=" + lastAcceptedVersion +
                ", lastAcceptedTerm=" + lastAcceptedTerm +
                ", sourceNode=" + sourceNode +
                ", targetNode=" + targetNode +
                '}';
        }
    }

    public abstract static class TermVersion extends TransportRequest implements Writeable {
        protected final DiscoveryNode sourceNode;
        protected final long term;
        protected final long version;

        public TermVersion(DiscoveryNode sourceNode, long term, long version) {
            assert term >= 0;
            assert version >= 0;

            this.sourceNode = sourceNode;
            this.term = term;
            this.version = version;
        }

        public TermVersion(StreamInput in) throws IOException {
            super(in);
            sourceNode = new DiscoveryNode(in);
            term = in.readLong();
            version = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            out.writeLong(term);
            out.writeLong(version);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

        public long getTerm() {
            return term;
        }

        public long getVersion() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TermVersion versionTerm = (TermVersion) o;

            if (term != versionTerm.term) return false;
            if (version != versionTerm.version) return false;
            return sourceNode.equals(versionTerm.sourceNode);
        }

        @Override
        public int hashCode() {
            int result = (int) (term ^ (term >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + sourceNode.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "TermVersion{" +
                "term=" + term +
                ", version=" + version +
                ", sourceNode=" + sourceNode +
                '}';
        }
    }

    public abstract static class VersionTermResponse extends TransportResponse {
        protected final long version;
        protected final long term;

        public VersionTermResponse(long version, long term) {
            assert version >= 0;
            assert term >= 0;

            this.version = version;
            this.term = term;
        }

        public VersionTermResponse(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            out.writeLong(term);
        }

        public long getVersion() {
            return version;
        }

        public long getTerm() {
            return term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionTermResponse response = (VersionTermResponse) o;

            if (version != response.version) return false;
            return term == response.term;
        }

        @Override
        public int hashCode() {
            int result = (int) (version ^ (version >>> 32));
            result = 31 * result + (int) (term ^ (term >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "VersionTermResponse{" +
                "version=" + version +
                ", term=" + term +
                '}';
        }
    }

    public static class LegislatorPublishResponse extends TransportResponse {
        private final PublishResponse publishResponse;
        private final Optional<Join> optionalJoin; // if vote was granted due to node having lower term

        public LegislatorPublishResponse(PublishResponse publishResponse, Optional<Join> optionalJoin) {
            this.publishResponse = publishResponse;
            this.optionalJoin = optionalJoin;
        }

        public LegislatorPublishResponse(StreamInput in) throws IOException {
            this.publishResponse = new PublishResponse(in);
            this.optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            publishResponse.writeTo(out);
            out.writeOptionalWriteable(optionalJoin.orElse(null));
        }

        public PublishResponse getPublishResponse() {
            return publishResponse;
        }

        public Optional<Join> getJoin() {
            return optionalJoin;
        }

    }

    public static class PublishResponse extends VersionTermResponse {

        public PublishResponse(long version, long term) {
            super(version, term);
        }

        public PublishResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "PublishResponse{" +
                "version=" + version +
                ", term=" + term +
                '}';
        }
    }

    public static class HeartbeatRequest extends TermVersion {

        public HeartbeatRequest(DiscoveryNode sourceNode, long term, long version) {
            super(sourceNode, term, version);
        }

        public HeartbeatRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "HeartbeatRequest{" +
                "term=" + term +
                ", version=" + version +
                '}';
        }
    }

    public static class HeartbeatResponse extends VersionTermResponse {

        public HeartbeatResponse(long version, long term) {
            super(version, term);
        }

        public HeartbeatResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "HeartbeatResponse{" +
                "version=" + version +
                ", term=" + term +
                '}';
        }
    }

    public static class ApplyCommit extends TermVersion {

        public ApplyCommit(DiscoveryNode sourceNode, long term, long version) {
            super(sourceNode, term, version);
        }

        public ApplyCommit(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public String toString() {
            return "ApplyCommit{" +
                "term=" + term +
                ", version=" + version +
                '}';
        }
    }

    public static class PublishRequest extends TransportRequest {

        private final ClusterState acceptedState;

        public PublishRequest(ClusterState acceptedState) {
            this.acceptedState = acceptedState;
        }

        public PublishRequest(StreamInput in, DiscoveryNode localNode) throws IOException {
            super(in);
            acceptedState = ClusterState.readFrom(in, localNode);
        }

        public ClusterState getAcceptedState() {
            return acceptedState;
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            PublishRequest that = (PublishRequest) o;
            return acceptedState != null ? acceptedState.equals(that.acceptedState) : that.acceptedState == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (acceptedState != null ? acceptedState.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "PublishRequest{" +
                "state=" + acceptedState + "}";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            acceptedState.writeTo(out);
        }
    }

    public static class StartJoinRequest extends TransportRequest {

        private final DiscoveryNode sourceNode;

        private final long term;

        public StartJoinRequest(DiscoveryNode sourceNode, long term) {
            this.sourceNode = sourceNode;
            this.term = term;
        }

        public StartJoinRequest(StreamInput input) throws IOException {
            super(input);
            this.sourceNode = new DiscoveryNode(input);
            this.term = input.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            out.writeLong(term);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

        public long getTerm() {
            return term;
        }

        @Override
        public String toString() {
            return "StartJoinRequest{" +
                "term=" + term +
                ",node=" + sourceNode + "}";
        }
    }

    public static class LeaderCheckResponse extends TransportResponse {
        private final long version;

        public LeaderCheckResponse(long version) {
            this.version = version;
        }

        public LeaderCheckResponse(StreamInput in) throws IOException {
            version = in.readLong();
        }

        @Override
        public String toString() {
            return "LeaderCheckResponse{" +
                "version=" + version +
                '}';
        }

        public long getVersion() {
            return version;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(version);
        }
    }

    public static class AbdicationRequest extends TransportRequest {

        private final DiscoveryNode sourceNode;
        private final long term;

        public AbdicationRequest(DiscoveryNode sourceNode, long term) {
            this.sourceNode = sourceNode;
            this.term = term;
        }

        public AbdicationRequest(StreamInput in) throws IOException {
            super(in);
            sourceNode = new DiscoveryNode(in);
            term = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            out.writeLong(term);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

        public long getTerm() {
            return term;
        }

    }

    public static class LeaderCheckRequest extends TransportRequest {

        private final DiscoveryNode sourceNode;

        public LeaderCheckRequest(DiscoveryNode sourceNode) {
            this.sourceNode = sourceNode;
        }

        public LeaderCheckRequest(StreamInput in) throws IOException {
            super(in);
            sourceNode = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

    }

    public static class PrejoinHandoverRequest extends TransportRequest {

        private final DiscoveryNode sourceNode;

        public PrejoinHandoverRequest(DiscoveryNode sourceNode) {
            this.sourceNode = sourceNode;
        }

        public PrejoinHandoverRequest(StreamInput in) throws IOException {
            super(in);
            sourceNode = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
        }

        public DiscoveryNode getSourceNode() {
            return sourceNode;
        }

    }
}

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
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ReverseRemoteAction extends ActionType<AcknowledgedResponse> {

    public static final ReverseRemoteAction INSTANCE = new ReverseRemoteAction();
    public static final String NAME = "cluster:connection/reverse";

    private ReverseRemoteAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public final String clusterName;
        public final DiscoveryNode discoNode;

        public Request(String clusterName, DiscoveryNode discoNode) {
            this.clusterName = clusterName;
            this.discoNode = discoNode;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            clusterName = in.readString();
            discoNode = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(clusterName);
            discoNode.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

    }

}

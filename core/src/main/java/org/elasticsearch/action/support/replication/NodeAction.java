package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;

public interface NodeAction<Request, Response> {
    void execute(DiscoveryNode node, Request request, ActionListener<Response> listener);
}

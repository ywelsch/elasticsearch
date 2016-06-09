package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.discovery.Discovery;

import java.util.List;

class DelegetingAckListener implements Discovery.AckListener {

    final private List<Discovery.AckListener> listeners;

    DelegetingAckListener(List<Discovery.AckListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void onNodeAck(DiscoveryNode node, @Nullable Throwable t) {
        for (Discovery.AckListener listener : listeners) {
            listener.onNodeAck(node, t);
        }
    }

    @Override
    public void onTimeout() {
        throw new UnsupportedOperationException("no timeout delegation");
    }
}

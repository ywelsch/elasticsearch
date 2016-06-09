package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

public class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {
    private final AckedClusterStateTaskListener listener;
    private final ESLogger logger;

    SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, ESLogger logger) {
        super(listener, logger);
        this.listener = listener;
        this.logger = logger;
    }

    @Override
    public boolean mustAck(DiscoveryNode discoveryNode) {
        return listener.mustAck(discoveryNode);
    }

    @Override
    public void onAllNodesAcked(@Nullable Throwable t) {
        try {
            listener.onAllNodesAcked(t);
        } catch (Exception e) {
            logger.error("exception thrown by listener while notifying on all nodes acked [{}]", e, t);
        }
    }

    @Override
    public void onAckTimeout() {
        try {
            listener.onAckTimeout();
        } catch (Exception e) {
            logger.error("exception thrown by listener while notifying on ack timeout", e);
        }
    }

    @Override
    public TimeValue ackTimeout() {
        return listener.ackTimeout();
    }
}

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Future;

class AckCountDownListener implements Discovery.AckListener {

    private static final ESLogger logger = Loggers.getLogger(AckCountDownListener.class);

    private final AckedClusterStateTaskListener ackedTaskListener;
    private final CountDown countDown;
    private final DiscoveryNodes nodes;
    private final long clusterStateVersion;
    private final Future<?> ackTimeoutCallback;
    private Throwable lastFailure;

    AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes,
                         ThreadPool threadPool) {
        this.ackedTaskListener = ackedTaskListener;
        this.clusterStateVersion = clusterStateVersion;
        this.nodes = nodes;
        int countDown = 0;
        for (DiscoveryNode node : nodes) {
            if (ackedTaskListener.mustAck(node)) {
                countDown++;
            }
        }
        //we always wait for at least 1 node (the master)
        countDown = Math.max(1, countDown);
        logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
        this.countDown = new CountDown(countDown);
        this.ackTimeoutCallback = threadPool.schedule(ackedTaskListener.ackTimeout(), ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                onTimeout();
            }
        });
    }

    @Override
    public void onNodeAck(DiscoveryNode node, @Nullable Throwable t) {
        if (!ackedTaskListener.mustAck(node)) {
            //we always wait for the master ack anyway
            if (!node.equals(nodes.getMasterNode())) {
                return;
            }
        }
        if (t == null) {
            logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
        } else {
            this.lastFailure = t;
            logger.debug("ack received from node [{}], cluster_state update (version: {})", t, node, clusterStateVersion);
        }

        if (countDown.countDown()) {
            logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
            FutureUtils.cancel(ackTimeoutCallback);
            ackedTaskListener.onAllNodesAcked(lastFailure);
        }
    }

    @Override
    public void onTimeout() {
        if (countDown.fastForward()) {
            logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
            ackedTaskListener.onAckTimeout();
        }
    }
}

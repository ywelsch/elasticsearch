package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.common.logging.ESLogger;

public class SafeClusterStateTaskListener implements ClusterStateTaskListener {
    private final ClusterStateTaskListener listener;
    private final ESLogger logger;

    SafeClusterStateTaskListener(ClusterStateTaskListener listener, ESLogger logger) {
        this.listener = listener;
        this.logger = logger;
    }

    @Override
    public void onFailure(String source, Throwable t) {
        try {
            listener.onFailure(source, t);
        } catch (Exception e) {
            logger.error("exception thrown by listener notifying of failure [{}] from [{}]", e, t, source);
        }
    }

    @Override
    public void onNoLongerMaster(String source) {
        try {
            listener.onNoLongerMaster(source);
        } catch (Exception e) {
            logger.error("exception thrown by listener while notifying no longer master from [{}]", e, source);
        }
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        try {
            listener.clusterStateProcessed(source, oldState, newState);
        } catch (Exception e) {
            logger.error(
                    "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" +
                            "{}\nnew cluster state:\n{}",
                    e,
                    source,
                    oldState.prettyPrint(),
                    newState.prettyPrint());
        }
    }
}

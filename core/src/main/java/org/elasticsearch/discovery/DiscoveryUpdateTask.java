package org.elasticsearch.discovery;

import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.common.Priority;

public abstract class DiscoveryUpdateTask extends LocalClusterUpdateTask {

    public DiscoveryUpdateTask() {
        this(Priority.NORMAL);
    }

    public DiscoveryUpdateTask(Priority priority) {
        super(priority);
    }

    @Override
    public final boolean isPublishingTask() {
        return true;
    }
}

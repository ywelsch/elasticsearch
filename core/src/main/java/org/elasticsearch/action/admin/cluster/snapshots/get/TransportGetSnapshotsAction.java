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

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportGetSnapshotsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetSnapshotsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
              GetSnapshotsRequest::new);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected GetSnapshotsResponse newResponse() {
        return new GetSnapshotsResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final GetSnapshotsRequest request, ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        try {
            final String repository = request.repository();
            List<SnapshotInfo> snapshotInfoBuilder = new ArrayList<>();
            if (isAllSnapshots(request.snapshots())) {
                snapshotInfoBuilder.addAll(snapshotsService.snapshots(repository, request.ignoreUnavailable()));
            } else if (isCurrentSnapshots(request.snapshots())) {
                snapshotInfoBuilder.addAll(snapshotsService.currentSnapshots(repository));
            } else {
                Map<String, SnapshotInfo> preFetched = null;
                final Set<String> toResolve = new LinkedHashSet<>(); // maintain order
                for (String snapshotOrPattern : request.snapshots()) {
                    if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                        toResolve.add(snapshotOrPattern);
                    } else {
                        if (preFetched == null) { // lazily load snapshots
                            preFetched = snapshotsService.snapshots(repository, request.ignoreUnavailable())
                                             .stream().collect(Collectors.toMap(s -> s.snapshotId().getName(), Function.identity()));
                        }
                        for (SnapshotInfo snapshot : preFetched.values()) {
                            if (Regex.simpleMatch(snapshotOrPattern, snapshot.snapshotId().getName())) {
                                toResolve.add(snapshot.snapshotId().getName());
                            }
                        }
                    }
                }
                final Map<String, SnapshotId> snapshotIds;
                // only resolve snapshot ids if preFetched is not set; if preFetched is set,
                // just use the SnapshotInfos we already have from preFetched
                if (preFetched == null) {
                    snapshotIds = snapshotsService.resolveSnapshotNames(repository,
                                                                        Arrays.asList(toResolve.toArray(new String[toResolve.size()])),
                                                                        request.ignoreUnavailable())
                                                  .stream().collect(Collectors.toMap(SnapshotId::getName, Function.identity()));
                } else {
                    snapshotIds = null;
                }
                for (String snapshotName : toResolve) {
                    final SnapshotInfo snapshotInfo;
                    if (preFetched != null) {
                        // if we have pre-fetched the snapshot infos, then use it to get the snapshot info
                        snapshotInfo = preFetched.get(snapshotName);
                    } else {
                        // otherwise, use the resolved snapshot ids to call the snapshots service to get the snapshot info
                        assert snapshotIds != null; // if preFetched isn't set, then snapshotIds must be set above
                        final SnapshotId snapshotId = snapshotIds.get(snapshotName);
                        if (snapshotId != null) {
                            snapshotInfo = snapshotsService.snapshot(repository, snapshotId);
                        } else {
                            snapshotInfo = null;
                        }
                    }
                    if (snapshotInfo != null) {
                        snapshotInfoBuilder.add(snapshotInfo);
                    } else if (request.ignoreUnavailable() == false) {
                        // Its possible that we didn't find a snapshot info for the given name, for example,
                        // if we had a pre fetched map, so we didn't need to resolve snapshot ids.  In this case,
                        // if the request does not permit ignoring unavailable snapshots, then throw an exception
                        // if we could not find the snapshot info.
                        throw new SnapshotMissingException(repository, snapshotName);
                    }
                }
            }
            listener.onResponse(new GetSnapshotsResponse(Collections.unmodifiableList(snapshotInfoBuilder)));
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshots(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }
}

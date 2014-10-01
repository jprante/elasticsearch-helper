package org.xbib.elasticsearch.action.support.replication.leader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.support.replication.Consistency;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TransportLeaderShardOperationAction<Request extends LeaderShardOperationRequest, Response extends ActionResponse>
        extends TransportAction<Request, Response> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportLeaderShardOperationAction.class.getSimpleName());

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    protected final IndicesService indicesService;

    protected final ShardStateAction shardStateAction;

    protected final TransportRequestOptions transportOptions;

    final String transportAction;

    final String executor;

    protected TransportLeaderShardOperationAction(Settings settings, String actionName, TransportService transportService,
                                                  ClusterService clusterService, IndicesService indicesService,
                                                  ThreadPool threadPool, ShardStateAction shardStateAction,
                                                  ActionFilters actionFilters) {
        super(settings, actionName, threadPool, actionFilters);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;

        this.transportAction = transportAction();
        this.transportOptions = transportOptions();
        this.executor = executor();

        transportService.registerHandler(transportAction, new LeaderOperationTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance();

    protected abstract String transportAction();

    protected abstract String executor();

    protected abstract Response shardOperationOnLeader(ClusterState clusterState, int quorom, LeaderOperationRequest shardRequest);

    protected abstract ShardIterator shards(ClusterState clusterState, Request request) throws ElasticsearchException;

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    protected boolean resolveRequest(ClusterState state, Request request, ActionListener<Response> listener) {
        request.index(state.metaData().concreteSingleIndex(request.index(), request.indicesOptions()));
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    protected boolean retryLeaderException(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    protected class AsyncShardOperationAction {

        private final ActionListener<Response> listener;

        private final Request request;

        private volatile ShardIterator shardIt;

        private final AtomicBoolean leaderOperationStarted = new AtomicBoolean();

        private volatile ClusterStateObserver observer;

        AsyncShardOperationAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            observer = new ClusterStateObserver(clusterService, request.timeout(), logger);
            doStart();
        }

        protected boolean doStart() throws ElasticsearchException {
            try {
                ClusterBlockException blockException = checkGlobalBlock(observer.observedState(), request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        }
                        retry(blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                if (!resolveRequest(observer.observedState(), request, listener)) {
                    return true;
                }
                blockException = checkRequestBlock(observer.observedState(), request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                        }
                        retry(blockException);
                        return false;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(observer.observedState(), request);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                listener.onFailure(e);
                return true;
            }
            if (shardIt.size() == 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("no shard instances known for shard [{}], scheduling a retry", shardIt.shardId());
                }
                retry(null);
                return false;
            }
            final int quorum = findQuorum(observer.observedState(), shardIt, request);
            if (quorum < 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("quorum not fulfilled for index {}, scheduling a retry", request.index());
                }
                retry(null);
                return false;
            }
            shardIt.reset();
            boolean foundLeader = false;
            ShardRouting shardRouting;
            while ((shardRouting = shardIt.nextOrNull()) != null) {
                final ShardRouting shard = shardRouting;
                if (!shard.primary()) {
                    continue;
                }
                if (!shard.active() || !observer.observedState().nodes().nodeExists(shard.currentNodeId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("leader shard [{}] is not yet active or we do not know the node it is assigned to [{}], scheduling a retry",
                                shard.shardId(), shard.currentNodeId());
                    }
                    retry(null);
                    return false;
                }
                if (!leaderOperationStarted.compareAndSet(false, true)) {
                    return true;
                }
                foundLeader = true;
                if (shard.currentNodeId().equals(observer.observedState().nodes().localNodeId())) {
                    try {
                        if (request.operationThreaded()) {
                            request.beforeLocalFork();
                            threadPool.executor(executor).execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        performOnLeader(shard, quorum, observer.observedState());
                                    } catch (Throwable t) {
                                        logger.error(t.getMessage(), t);
                                        listener.onFailure(t);
                                    }
                                }
                            });
                        } else {
                            performOnLeader(shard, quorum, observer.observedState());
                        }
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                        listener.onFailure(t);
                    }
                } else {
                    DiscoveryNode node = observer.observedState().nodes().get(shard.currentNodeId());
                    transportService.sendRequest(node, transportAction, request, transportOptions, new BaseTransportResponseHandler<Response>() {

                        @Override
                        public Response newInstance() {
                            return newResponseInstance();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.error(exp.getMessage(), exp);
                            if (exp.unwrapCause() instanceof ConnectTransportException
                                    || exp.unwrapCause() instanceof NodeClosedException
                                    || retryLeaderException(exp)) {
                                leaderOperationStarted.set(false);
                                if (logger.isTraceEnabled()) {
                                    logger.trace("received an error from node the leader was assigned to ({}), scheduling a retry", exp.getMessage());
                                }
                                retry(null);
                            } else {
                                listener.onFailure(exp);
                            }
                        }
                    });
                }
                break;
            }
            if (!foundLeader) {
                if (logger.isTraceEnabled()) {
                    logger.trace("couldn't find a leader shard, scheduling for retry");
                }
                retry(null);
                return false;
            }
            return true;
        }

        private void retry(@Nullable final Throwable failure) {
            if (observer.isTimedOut()) {
                return;
            }
            request.beforeLocalFork();
            request.operationThreaded(true);
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart();
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    if (doStart()) {
                        return;
                    }
                    raiseTimeoutFailure(timeout, failure);
                }
            });
        }

        private void raiseTimeoutFailure(TimeValue timeout, @Nullable Throwable failure) {
            if (failure == null) {
                if (shardIt == null) {
                    failure = new UnavailableShardsException(null, "no available shards, timeout waiting for [" + timeout + "], request: " + request.toString());
                } else {
                    failure = new UnavailableShardsException(shardIt.shardId(), "[" + shardIt.size() + "] shardIt, [" + shardIt.sizeActive() + "] active : Timeout waiting for [" + timeout + "], request: " + request.toString());
                }
            }
            listener.onFailure(failure);
        }

        private void performOnLeader(final ShardRouting shard, int quorum, ClusterState clusterState) {
            try {
                LeaderOperationRequest leaderOperationRequest = new LeaderOperationRequest(System.currentTimeMillis(), shard.id(), request);
                Response response = shardOperationOnLeader(clusterState, quorum, leaderOperationRequest);
                listener.onResponse(response);
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                if (retryLeaderException(e)) {
                    leaderOperationStarted.set(false);
                    if (logger.isTraceEnabled()) {
                        logger.trace("error while performing operation on leader {}, scheduling a retry", e);
                    }
                    retry(e);
                    return;
                }
                if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(shard.shortSummary() + ": failed to execute [" + request + "]", e);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(shard.shortSummary() + ": failed to execute [" + request + "]", e);
                    }
                }
                listener.onFailure(e);
            }
        }
    }

    public int findQuorum(ClusterState clusterState, ShardIterator shardIt, Request request) {
        if (request.requiredConsistency() == Consistency.IGNORE) {
            return 0;
        }
        int numberOfDataNodes = 0;
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (node.isDataNode()) {
                numberOfDataNodes++;
            }
        }
        // single node, do not care about replica
        if (numberOfDataNodes == 1) {
            return 0;
        }
        int replicaLevelOfIndex = clusterState.metaData().index(request.index()).getNumberOfReplicas();
        // no replica defined, so nothing to check
        if (replicaLevelOfIndex == 0) {
            return 0;
        }
        int replicaLevel = findReplicaLevel(shardIt) + 1;
        switch (request.requiredConsistency) {
            case ONE:
                if (replicaLevel >= 1 && replicaLevelOfIndex >= 1) {
                    return 1;
                }
                break;
            case QUORUM:
                int quorum = (replicaLevelOfIndex / 2) + 1;
                if (replicaLevel >= quorum) {
                    return quorum;
                }
                break;
            case ALL:
                if (replicaLevel == replicaLevelOfIndex) {
                    return replicaLevel;
                }
                break;
        }
        // quorum not matched - we have a problem
        return -1;
    }

    private int findReplicaLevel(ShardIterator shardIt) {
        int replicaLevel = 0;
        shardIt.reset();
        ShardRouting shard;
        while ((shard = shardIt.nextOrNull()) != null) {
            if (shard.unassigned()) {
                continue;
            }
            boolean doOnlyOnRelocating = false;
            if (shard.primary()) {
                if (shard.relocating()) {
                    doOnlyOnRelocating = true;
                } else {
                    continue;
                }
            }
            String nodeId = !doOnlyOnRelocating ? shard.currentNodeId() : shard.relocating() ? shard.relocatingNodeId() : null;
            if (nodeId == null) {
                continue;
            }
            replicaLevel++;
        }
        return replicaLevel;
    }


    protected class LeaderOperationRequest implements Streamable {

        private long startTime;

        private int shardId;

        private Request request;

        public LeaderOperationRequest(long startTime, int shardId, Request request) {
            this.startTime = startTime;
            this.shardId = shardId;
            this.request = request;
        }

        public long startTime() {
            return startTime;
        }

        public int shardId() {
            return shardId;
        }

        public Request request() {
            return request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            shardId = in.readVInt();
            request = newRequestInstance();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shardId);
            request.writeTo(out);
        }
    }

    class LeaderOperationTransportHandler extends BaseTransportRequestHandler<Request> {

        @Override
        public Request newInstance() {
            return newRequestInstance();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            request.operationThreaded(true);
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                        logger.error(e.getMessage(), e);
                    } catch (Throwable e1) {
                        logger.warn("failed to send response for " + transportAction, e1);
                    }
                }
            });
        }
    }

}

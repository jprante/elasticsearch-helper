package org.xbib.elasticsearch.action.ingest.leader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.ingest.Consistency;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransportLeaderShardIngestAction
        extends TransportAction<IngestLeaderShardRequest, IngestLeaderShardResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportLeaderShardIngestAction.class.getSimpleName());
    final String transportAction;
    final String executor;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportRequestOptions transportOptions;

    @Inject
    public TransportLeaderShardIngestAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            IndicesService indicesService, ThreadPool threadPool,
                                            ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IngestAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportAction = transportAction();
        this.transportOptions = transportOptions();
        this.executor = executor();
        transportService.registerRequestHandler(transportAction, IngestLeaderShardRequest.class,
                ThreadPool.Names.SAME, new LeaderOperationTransportHandler());
    }

    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    protected TransportRequestOptions transportOptions() {
        return IngestAction.INSTANCE.transportOptions(settings);
    }

    protected IngestLeaderShardRequest newRequestInstance() {
        return new IngestLeaderShardRequest();
    }

    protected IngestLeaderShardResponse newResponseInstance() {
        return new IngestLeaderShardResponse();
    }

    protected String transportAction() {
        return IngestAction.NAME + "/shard/leader";
    }

    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestLeaderShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestLeaderShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    protected ShardIterator shards(ClusterState clusterState, IngestLeaderShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.getShardId().id()).shardsIt();
    }

    protected IngestLeaderShardResponse shardOperationOnLeader(ClusterState clusterState, int replicaLevel, LeaderOperationRequest shardRequest) {
        final long t0 = shardRequest.startTime();
        final IngestLeaderShardRequest request = shardRequest.request();
        int successCount = 0;
        List<IngestActionFailure> failures = new LinkedList<>();
        int size = request.getActionRequests().size();
        long[] versions = new long[size];
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        for (int i = 0; i < size; i++) {
            ActionRequest<?> actionRequest = request.getActionRequests().get(i);
            if (actionRequest instanceof IndexRequest) {
                try {
                    IndexRequest indexRequest = (IndexRequest) actionRequest;
                    long version = indexOperationOnLeader(indexRequest, request);
                    versions[i] = indexRequest.version();
                    indexRequest.version(indexRequest.version() == Versions.MATCH_ANY ? Versions.MATCH_ANY : version);
                    successCount++;
                } catch (Throwable e) {
                    if (retryLeaderException(e)) {
                        for (int j = 0; j < i; j++) {
                            ((IndexRequest) request.getActionRequests().get(j)).version(versions[j]);
                        }
                        logger.error(e.getMessage(), e);
                        throw new ElasticsearchException(e.getMessage(), e);
                    }
                    logger.error("[{}][{}] failed to execute ingest (index) {}", e, request.index(), shardRequest.shardId(), actionRequest);
                    failures.add(new IngestActionFailure(request.getIngestId(), request.getShardId(), ExceptionsHelper.detailedMessage(e)));
                    request.getActionRequests().set(i, null);
                }
            } else if (actionRequest instanceof DeleteRequest) {
                try {
                    IndexShard indexShard = indexService.shardSafe(shardRequest.shardId());
                    DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                    Engine.Delete delete = indexShard.prepareDeleteOnPrimary(deleteRequest.type(), deleteRequest.id(), deleteRequest.version(), deleteRequest.versionType());
                    indexShard.delete(delete);
                    deleteRequest.version(deleteRequest.version() == Versions.MATCH_ANY ? Versions.MATCH_ANY : delete.version());
                    successCount++;
                } catch (Throwable e) {
                    if (retryLeaderException(e)) {
                        for (int j = 0; j < i; j++) {
                            ((DeleteRequest) request.getActionRequests().get(j)).version(versions[j]);
                        }
                        logger.error(e.getMessage(), e);
                        throw new ElasticsearchException(e.getMessage(), e);
                    }
                    logger.error("[{}][{}] failed to execute ingest (delete) {}", e, request.index(), shardRequest.shardId(), actionRequest);
                    failures.add(new IngestActionFailure(request.getIngestId(), request.getShardId(), ExceptionsHelper.detailedMessage(e)));
                    request.getActionRequests().set(i, null);
                }
            }
        }
        int quorumShards = findQuorum(clusterState, shards(clusterState, request), request);
        return new IngestLeaderShardResponse()
                .setTookInMillis(System.currentTimeMillis() - t0)
                .setIngestId(request.getIngestId())
                .setShardId(request.getShardId())
                .setSuccessCount(successCount)
                .setQuorumShards(quorumShards)
                .setActionRequests(request.getActionRequests())
                .setFailures(failures);
    }

    private long indexOperationOnLeader(IndexRequest indexRequest,
                                        IngestLeaderShardRequest request) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, indexRequest.source())
                .type(indexRequest.type())
                .id(indexRequest.id())
                .routing(indexRequest.routing())
                .parent(indexRequest.parent())
                .timestamp(indexRequest.timestamp())
                .ttl(indexRequest.ttl());
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.getShardId().id());
        if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndexOnPrimary(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    false);
            indexShard.index(index);
            return index.version();
        } else if (indexRequest.opType() == IndexRequest.OpType.CREATE) {
            Engine.Create create = indexShard.prepareCreateOnPrimary(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    false,
                    indexRequest.autoGeneratedId());
            indexShard.create(create);
            return create.version();
        } else {
            logger.error("unknown op type " + indexRequest.opType());
            return 0;
        }

    }

    @Override
    protected void doExecute(IngestLeaderShardRequest request, ActionListener<IngestLeaderShardResponse> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected boolean retryLeaderException(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    public int findQuorum(ClusterState clusterState, ShardIterator shardIt, IngestLeaderShardRequest request) {
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
        switch (request.requiredConsistency()) {
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

    protected class AsyncShardOperationAction {

        private final ActionListener<IngestLeaderShardResponse> listener;

        private final IngestLeaderShardRequest request;
        private final AtomicBoolean leaderOperationStarted = new AtomicBoolean();
        private volatile ShardIterator shardIt;
        private volatile ClusterStateObserver observer;

        AsyncShardOperationAction(IngestLeaderShardRequest request, ActionListener<IngestLeaderShardResponse> listener) {
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
                    transportService.sendRequest(node, transportAction, request, transportOptions, new BaseTransportResponseHandler<IngestLeaderShardResponse>() {

                        @Override
                        public IngestLeaderShardResponse newInstance() {
                            return newResponseInstance();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(IngestLeaderShardResponse response) {
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
                listener.onFailure(failure);
                return;
            }
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
                IngestLeaderShardResponse response = shardOperationOnLeader(clusterState, quorum, leaderOperationRequest);
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

    protected class LeaderOperationRequest implements Streamable {

        private long startTime;

        private int shardId;

        private IngestLeaderShardRequest request;

        public LeaderOperationRequest(long startTime, int shardId, IngestLeaderShardRequest request) {
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

        public IngestLeaderShardRequest request() {
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

    class LeaderOperationTransportHandler extends TransportRequestHandler<IngestLeaderShardRequest> {

        @Override
        public void messageReceived(final IngestLeaderShardRequest request, final TransportChannel channel) throws Exception {
            execute(request, new ActionListener<IngestLeaderShardResponse>() {
                @Override
                public void onResponse(IngestLeaderShardResponse result) {
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

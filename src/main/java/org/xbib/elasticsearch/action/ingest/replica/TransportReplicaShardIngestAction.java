package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TransportReplicaShardIngestAction
        extends TransportAction<IngestReplicaShardRequest, TransportReplicaShardIngestAction.ReplicaOperationResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportReplicaShardIngestAction.class.getSimpleName());

    final String transportAction;

    final String executor;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ShardStateAction shardStateAction;

    private final TransportRequestOptions transportOptions;

    @Inject
    public TransportReplicaShardIngestAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                             ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IngestAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.transportAction = transportAction();
        this.transportOptions = transportOptions();
        this.executor = executor();
        transportService.registerRequestHandler(transportAction, ReplicaOperationRequest.class,
                ThreadPool.Names.SAME, new ReplicaOperationTransportHandler());
    }

    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    protected TransportRequestOptions transportOptions() {
        return IngestAction.INSTANCE.transportOptions(settings);
    }

    protected IngestReplicaShardResponse newResponseInstance() {
        return new IngestReplicaShardResponse();
    }

    protected String transportAction() {
        return IngestAction.NAME + ".shard.replica";
    }

    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestReplicaShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestReplicaShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    protected ShardIterator shards(ClusterState clusterState, IngestReplicaShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId().id()).shardsIt();
    }

    protected ReplicaOperationResponse newReplicaResponseInstance() {
        return new ReplicaOperationResponse();
    }

    protected IngestReplicaShardResponse shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        final long t0 = shardRequest.startTime();
        final IngestReplicaShardRequest request = shardRequest.request();
        final IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request().index()).shardSafe(shardRequest.shardId());
        int successCount = 0;
        List<IngestActionFailure> failure = new LinkedList<>();
        int size = request.actionRequests().size();
        for (int i = 0; i < size; i++) {
            ActionRequest<?> actionRequest = request.actionRequests().get(i);
            if (actionRequest == null) {
                continue;
            }
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                try {
                    indexOperationOnReplica(indexShard, indexRequest);
                    successCount++;
                } catch (Throwable e) {
                    failure.add(new IngestActionFailure(request.ingestId(), request.shardId(), ExceptionsHelper.detailedMessage(e)));
                }
            } else if (actionRequest instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                try {
                    Engine.Delete delete = indexShard.prepareDeleteOnReplica(deleteRequest.type(),
                            deleteRequest.id(),
                            deleteRequest.version(),
                            deleteRequest.versionType());
                    indexShard.delete(delete);
                    successCount++;
                } catch (Throwable e) {
                    failure.add(new IngestActionFailure(request.ingestId(), request.shardId(), ExceptionsHelper.detailedMessage(e)));
                }
            }
        }
        return new IngestReplicaShardResponse(request.ingestId(), request.shardId(), shardRequest.replicaId(),
                successCount, System.currentTimeMillis() - t0, failure);
    }

    private void indexOperationOnReplica(IndexShard indexShard, IndexRequest indexRequest) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, indexRequest.source())
                .type(indexRequest.type())
                .id(indexRequest.id())
                .routing(indexRequest.routing())
                .parent(indexRequest.parent())
                .timestamp(indexRequest.timestamp())
                .ttl(indexRequest.ttl());
        if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndexOnReplica(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    false);
            indexShard.index(index);
        } else {
            Engine.Create create = indexShard.prepareCreateOnReplica(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    false,
                    indexRequest.autoGeneratedId());
            indexShard.create(create);
        }
    }

    @Override
    protected void doExecute(IngestReplicaShardRequest request, ActionListener<ReplicaOperationResponse> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    boolean ignoreReplicaException(Throwable e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof VersionConflictEngineException;
    }

    private void failReplicaIfNeeded(String index, int shardId, Throwable t) {
        if (!ignoreReplicaException(t)) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("ignoring failed replica [{}][{}] because index was already removed", index, shardId);
                }
                return;
            }
            IndexShard indexShard = indexService.shard(shardId);
            if (indexShard == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("ignoring failed replica [{}][{}] because index was already removed", index, shardId);
                }
                return;
            }
            indexShard.failShard(transportAction + " failed on replica", t);
        }
    }

    protected class AsyncShardOperationAction {

        private final IngestReplicaShardRequest request;

        private final ActionListener<ReplicaOperationResponse> listener;

        private final ReplicaOperationResponse response;

        private volatile ShardIterator shardIt;

        private volatile ClusterStateObserver observer;

        AsyncShardOperationAction(IngestReplicaShardRequest request, ActionListener<ReplicaOperationResponse> listener) {
            this.request = request;
            this.listener = listener;
            this.response = newReplicaResponseInstance();
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
            ShardRouting shard;
            ClusterState newState = clusterService.state();
            ShardRouting newLeaderShard = null;
            if (observer.observedState() != newState) {
                shardIt.reset();
                ShardRouting originalLeaderShard = null;
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        originalLeaderShard = shard;
                        break;
                    }
                }
                if (originalLeaderShard == null || !originalLeaderShard.active()) {
                    throw new ElasticsearchException("unexpected state, failed to find leader shard on an index operation that succeeded");
                }
                observer.reset(newState);
                shardIt = shards(newState, request);
                while ((shard = shardIt.nextOrNull()) != null) {
                    if (shard.primary()) {
                        if (originalLeaderShard.currentNodeId().equals(shard.currentNodeId())) {
                            newLeaderShard = null;
                        } else {
                            newLeaderShard = shard;
                        }
                        break;
                    }
                }
                shardIt.reset();
            }
            int replicaCounter = shardIt.assignedReplicasIncludingRelocating();
            if (newLeaderShard != null) {
                replicaCounter++;
            }
            if (replicaCounter == 0) {
                return true;
            }
            final IndexMetaData indexMetaData = observer.observedState().metaData().index(request.index());
            if (newLeaderShard != null) {
                logger.debug("we have a new leader shard?");
                //performOnReplica(newLeaderShard, replicaCounter, newLeaderShard.currentNodeId(), indexMetaData);
            }
            List<ReplicaInfo> infos = new LinkedList<>();
            int replicaLevel = 1;
            shardIt.reset();
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
                infos.add(new ReplicaInfo(nodeId, shard, replicaLevel));
                replicaLevel++;
            }
            final AtomicLong replicas = new AtomicLong(infos.size());
            for (ReplicaInfo info : infos) {
                final ReplicaOperationRequest replicaRequest = new ReplicaOperationRequest(System.currentTimeMillis(),
                        request.index(), info.shardRouting().shardId().id(), info.replicaLevel(), request);
                final String nodeId = info.nodeId();
                final ShardRouting shardRouting = info.shardRouting();
                if (!observer.observedState().nodes().nodeExists(nodeId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("node {} no longer exists", nodeId);
                    }
                } else {
                    if (nodeId.equals(observer.observedState().nodes().localNodeId())) {
                        if (request.operationThreaded()) {
                            try {
                                threadPool.executor(executor).execute(new AbstractRunnable() {

                                    @Override
                                    public void onFailure(Throwable t) {
                                        logger.error(t.getMessage(), t);
                                        failReplicaIfNeeded(shardRouting.index(), shardRouting.id(), t);
                                    }

                                    @Override
                                    protected void doRun() throws Exception {
                                        response.add(shardOperationOnReplica(replicaRequest));
                                        if (replicas.decrementAndGet() == 0) {
                                            listener.onResponse(response);
                                        }
                                    }

                                    @Override
                                    public boolean isForceExecution() {
                                        return true;
                                    }
                                });
                            } catch (Throwable e) {
                                logger.error(e.getMessage(), e);
                                failReplicaIfNeeded(shard.index(), shard.id(), e);
                            }
                        } else {
                            try {
                                response.add(shardOperationOnReplica(replicaRequest));
                                if (replicas.decrementAndGet() == 0) {
                                    listener.onResponse(response);
                                }
                            } catch (Throwable e) {
                                logger.error(e.getMessage(), e);
                                failReplicaIfNeeded(shard.index(), shard.id(), e);
                            }
                        }
                    } else {
                        final DiscoveryNode node = observer.observedState().nodes().get(nodeId);
                        transportService.sendRequest(node, transportAction, replicaRequest, transportOptions, new BaseTransportResponseHandler<IngestReplicaShardResponse>() {
                            @Override
                            public IngestReplicaShardResponse newInstance() {
                                return newResponseInstance();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public void handleResponse(IngestReplicaShardResponse result) {
                                response.add(result);
                                if (replicas.decrementAndGet() == 0) {
                                    listener.onResponse(response);
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.error(exp.getMessage(), exp);
                                if (!ignoreReplicaException(exp.unwrapCause())) {
                                    logger.warn("failed to perform " + transportAction + " on replica " + node + shardIt.shardId(), exp);
                                    shardStateAction.shardFailed(shardRouting, indexMetaData.getIndexUUID(),
                                            "failed to perform [" + transportAction + "] on replica, message [" + ExceptionsHelper.detailedMessage(exp) + "]", exp);
                                } else {
                                    logger.error(exp.getMessage(), exp);
                                }
                                listener.onFailure(exp);
                            }
                        });
                    }
                }
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

        void raiseTimeoutFailure(TimeValue timeout, @Nullable Throwable failure) {
            if (failure == null) {
                if (shardIt == null) {
                    failure = new UnavailableShardsException(null, "no available shards, timeout waiting for [" + timeout + "], request: " + request.toString());
                } else {
                    failure = new UnavailableShardsException(shardIt.shardId(), "[" + shardIt.size() + "] shardIt, [" + shardIt.sizeActive() + "] active : Timeout waiting for [" + timeout + "], request: " + request.toString());
                }
            }
            listener.onFailure(failure);
        }
    }

    class ReplicaInfo {
        private String nodeId;

        private ShardRouting shardRouting;

        private int replicaLevel;

        ReplicaInfo(String nodeId, ShardRouting shardRouting, int replicaLevel) {
            this.nodeId = nodeId;
            this.shardRouting = shardRouting;
            this.nodeId = nodeId;
            this.replicaLevel = replicaLevel;
        }

        public String nodeId() {
            return nodeId;
        }

        public ShardRouting shardRouting() {
            return shardRouting;
        }

        public int replicaLevel() {
            return replicaLevel;
        }
    }

    public class ReplicaOperationResponse extends ActionResponse {

        private final List<IngestReplicaShardResponse> responses = new LinkedList<>();

        public ReplicaOperationResponse add(IngestReplicaShardResponse response) {
            responses.add(response);
            return this;
        }

        public List<IngestReplicaShardResponse> responses() {
            return responses;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            for (int i = 0; i < in.readVInt(); i++) {
                IngestReplicaShardResponse response = newResponseInstance();
                response.readFrom(in);
                responses.add(response);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(responses.size());
            for (IngestReplicaShardResponse response : responses) {
                response.writeTo(out);
            }
        }

        public String toString() {
            return responses.toString();
        }
    }

    class ReplicaOperationTransportHandler extends TransportRequestHandler<ReplicaOperationRequest> {

        @Override
        public void messageReceived(final ReplicaOperationRequest request, final TransportChannel channel) throws Exception {
            try {
                ActionResponse response = shardOperationOnReplica(request);
                channel.sendResponse(response);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
                channel.sendResponse(t);
            }
        }
    }
}

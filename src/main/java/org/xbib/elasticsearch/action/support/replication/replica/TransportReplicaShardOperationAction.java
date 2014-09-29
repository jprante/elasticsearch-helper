package org.xbib.elasticsearch.action.support.replication.replica;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public abstract class TransportReplicaShardOperationAction<Request extends ReplicaShardOperationRequest, Response extends ActionResponse, ReplicaResponse extends TransportReplicaShardOperationAction.ReplicaOperationResponse>
        extends TransportAction<Request, ReplicaResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportReplicaShardOperationAction.class.getSimpleName());

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    protected final IndicesService indicesService;

    protected final ShardStateAction shardStateAction;

    protected final TransportRequestOptions transportOptions;

    final String transportAction;

    final String executor;

    protected TransportReplicaShardOperationAction(Settings settings, String actionName, TransportService transportService,
                                                   ClusterService clusterService, IndicesService indicesService,
                                                   ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings, actionName, threadPool);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;

        this.transportAction = transportAction();
        this.transportOptions = transportOptions();
        this.executor = executor();

        transportService.registerHandler(transportAction, new ReplicaOperationTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<ReplicaResponse> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance();

    protected abstract ReplicaResponse newReplicaResponseInstance();

    protected abstract String executor();

    protected abstract String transportAction();

    protected abstract Response shardOperationOnReplica(ReplicaOperationRequest shardRequest);

    protected abstract ShardIterator shards(ClusterState clusterState, Request request) throws ElasticsearchException;

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request);

    protected boolean resolveRequest(ClusterState state, Request request) {
        request.index(state.metaData().concreteSingleIndex(request.index()));
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    boolean ignoreReplicaException(Throwable e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof VersionConflictEngineException;
    }

    protected class AsyncShardOperationAction {

        private final Request request;

        private final ActionListener<ReplicaResponse> listener;

        private final ReplicaResponse response;

        private volatile ShardIterator shardIt;

        private volatile ClusterStateObserver observer;

        AsyncShardOperationAction(Request request, ActionListener<ReplicaResponse> listener) {
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
                if (!resolveRequest(observer.observedState(), request)) {
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
                    throw new ElasticsearchIllegalStateException("unexpected state, failed to find leader shard on an index operation that succeeded");
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
            List<ReplicaInfo> infos = newLinkedList();
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
                                    public void run() {
                                        try {
                                            response.add(shardOperationOnReplica(replicaRequest));
                                            if (replicas.decrementAndGet() == 0) {
                                                listener.onResponse(response);
                                            }
                                        } catch (Throwable e) {
                                            failReplicaIfNeeded(shardRouting.index(), shardRouting.id(), e);
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
                        transportService.sendRequest(node, transportAction, replicaRequest, transportOptions, new BaseTransportResponseHandler<Response>() {
                            @Override
                            public Response newInstance() {
                                return newResponseInstance();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public void handleResponse(Response result) {
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
                                    shardStateAction.shardFailed(shardRouting, indexMetaData.getUUID(),
                                            "failed to perform [" + transportAction + "] on replica, message [" + ExceptionsHelper.detailedMessage(exp) + "]");
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

    protected class ReplicaOperationRequest extends TransportRequest implements Streamable {

        private long startTime;

        private String index;

        private int shardId;

        private int replicaId;

        private Request request;

        ReplicaOperationRequest() {
        }

        public ReplicaOperationRequest(long startTime, String index, int shardId, int replicaId, Request request) {
            this.startTime = startTime;
            this.index = index;
            this.shardId = shardId;
            this.replicaId = replicaId;
            this.request = request;
        }

        public long startTime() {
            return startTime;
        }

        public String index() {
            return index;
        }

        public int shardId() {
            return shardId;
        }

        public int replicaId() {
            return replicaId;
        }

        public Request request() {
            return request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            startTime = in.readLong();
            index = in.readString();
            shardId = in.readVInt();
            replicaId = in.readVInt();
            request = newRequestInstance();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(startTime);
            out.writeString(index);
            out.writeVInt(shardId);
            out.writeVInt(replicaId);
            request.writeTo(out);
        }
    }

    public class ReplicaOperationResponse extends ActionResponse {

        private final List<Response> responses = newLinkedList();

        public ReplicaOperationResponse add(Response response) {
            responses.add(response);
            return this;
        }

        public List<Response> responses() {
            return responses;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            for (int i = 0; i < in.readVInt(); i++) {
                Response response = newResponseInstance();
                response.readFrom(in);
                responses.add(response);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(responses.size());
            for (Response response : responses) {
                response.writeTo(out);
            }
        }

        public String toString() {
            return responses.toString();
        }
    }

    class ReplicaOperationTransportHandler extends BaseTransportRequestHandler<ReplicaOperationRequest> {

        @Override
        public ReplicaOperationRequest newInstance() {
            return new ReplicaOperationRequest();
        }

        @Override
        public String executor() {
            return executor;
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }

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

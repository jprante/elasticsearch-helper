package org.xbib.elasticsearch.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.index.leader.TransportLeaderShardIndexAction;
import org.xbib.elasticsearch.action.index.replica.IndexReplicaShardRequest;
import org.xbib.elasticsearch.action.index.replica.TransportReplicaShardIndexAction;
import org.xbib.elasticsearch.action.support.replication.replica.TransportReplicaShardOperationAction;

import java.util.concurrent.atomic.AtomicInteger;

public class TransportIndexAction extends TransportAction<IndexRequest, IndexResponse> {

    private final ClusterService clusterService;

    private final boolean allowIdGeneration;

    private final TransportLeaderShardIndexAction transportLeaderShardIndexAction;

    private final TransportReplicaShardIndexAction transportReplicaShardIndexAction;

    @Inject
    public TransportIndexAction(Settings settings, ThreadPool threadPool,
                                TransportService transportService, ClusterService clusterService,
                                TransportLeaderShardIndexAction transportLeaderShardIndexAction,
                                TransportReplicaShardIndexAction transportReplicaShardIndexAction,
                                ActionFilters actionFilters) {
        super(settings, IndexAction.NAME, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.transportLeaderShardIndexAction = transportLeaderShardIndexAction;
        this.transportReplicaShardIndexAction = transportReplicaShardIndexAction;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);

        transportService.registerHandler(IndexAction.NAME, new IndexTransportHandler());
    }

    @Override
    protected void doExecute(final IndexRequest indexRequest, final ActionListener<IndexResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);
        try {
            MetaData metaData = clusterState.metaData();
            indexRequest.index(clusterState.metaData().concreteSingleIndex(indexRequest.index(), indexRequest.indicesOptions()));
            MappingMetaData mappingMd = null;
            if (metaData.hasIndex(indexRequest.index())) {
                mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
            }
            indexRequest.process(metaData, indexRequest.index(), mappingMd, allowIdGeneration);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            listener.onFailure(e);
            return;
        }
        final AtomicInteger counter = new AtomicInteger(1);
        final IndexResponse response = new IndexResponse();
        transportLeaderShardIndexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                int quorumShards = indexResponse.getQuorumShards();
                response.setIndex(indexRequest.index())
                        .setType(indexRequest.type())
                        .setId(indexRequest.id())
                        .setVersion(indexResponse.getVersion())
                        .setQuorumShards(quorumShards);
                ShardId shardId = clusterService.operationRouting().indexShards(clusterService.state(),
                        indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                if (quorumShards < 0) {
                    response.setFailure(new IndexActionFailure(shardId, "quorum not reached"));
                } else if (quorumShards > 0) {
                    counter.incrementAndGet();
                    IndexReplicaShardRequest indexReplicaShardRequest = new IndexReplicaShardRequest(shardId, indexRequest);
                    transportReplicaShardIndexAction.execute(indexReplicaShardRequest, new ActionListener<TransportReplicaShardOperationAction.ReplicaOperationResponse>() {
                        @Override
                        public void onResponse(TransportReplicaShardOperationAction.ReplicaOperationResponse replicaOperationResponse) {
                            response.addReplicaResponses(replicaOperationResponse.responses());
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(response);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error(e.getMessage(), e);
                            listener.onFailure(e);
                        }
                    });
                }
                if (counter.decrementAndGet() == 0) {
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error(e.getMessage(), e);
                listener.onFailure(e);
            }
        });
    }

    class IndexTransportHandler extends BaseTransportRequestHandler<IndexRequest> {

        @Override
        public IndexRequest newInstance() {
            return new IndexRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final IndexRequest request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            request.operationThreaded(true);
            execute(request, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse result) {
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
                    } catch (Exception e1) {
                        logger.warn("failed to send error response for action [" + IndexAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }
    }
}

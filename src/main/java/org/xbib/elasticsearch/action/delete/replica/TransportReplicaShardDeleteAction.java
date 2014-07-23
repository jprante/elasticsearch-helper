package org.xbib.elasticsearch.action.delete.replica;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.delete.DeleteAction;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.support.replication.replica.TransportReplicaShardOperationAction;

public class TransportReplicaShardDeleteAction
        extends TransportReplicaShardOperationAction<DeleteReplicaShardRequest, DeleteReplicaShardResponse, TransportReplicaShardOperationAction.ReplicaOperationResponse> {

    @Inject
    public TransportReplicaShardDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings, DeleteAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override
    protected DeleteReplicaShardRequest newRequestInstance() {
        return new DeleteReplicaShardRequest();
    }

    @Override
    protected DeleteReplicaShardResponse newResponseInstance() {
        return new DeleteReplicaShardResponse();
    }

    @Override
    protected String transportAction() {
        return DeleteAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteReplicaShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteReplicaShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, DeleteReplicaShardRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.request().index(), request.request().type(),
                        request.request().id(), request.request().routing());
    }

    @Override
    protected ReplicaOperationResponse newReplicaResponseInstance() {
        return new ReplicaOperationResponse();
    }

    @Override
    protected DeleteReplicaShardResponse shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        final long t0 = shardRequest.startTime();
        DeleteRequest request = shardRequest.request().request();
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request().index()).shardSafe(shardRequest.shardId());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), request.versionType(), Engine.Operation.Origin.REPLICA);
        indexShard.delete(delete);
        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_delete").force(false));
            } catch (Exception e) {
                // ignore
            }
        }
        return new DeleteReplicaShardResponse(shardRequest.request().shardId(), shardRequest.replicaId(),
                System.currentTimeMillis() - t0);
    }
}

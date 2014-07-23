package org.xbib.elasticsearch.action.delete.leader;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.delete.DeleteAction;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.delete.DeleteResponse;
import org.xbib.elasticsearch.action.support.replication.leader.TransportLeaderShardOperationAction;

public class TransportLeaderShardDeleteAction extends TransportLeaderShardOperationAction<DeleteRequest, DeleteResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportLeaderShardDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                            TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction) {
        super(settings, DeleteAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = new AutoCreateIndex(settings);
    }

    @Override
    protected void doExecute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            request.beforeLocalFork();
            createIndexAction.execute(new CreateIndexRequest(request.index()).cause("auto(delete api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        innerExecute(request, listener);
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(request, listener);
        }
    }

    @Override
    protected boolean resolveRequest(ClusterState state, DeleteRequest request, ActionListener<DeleteResponse> indexResponseActionListener) {
        request.routing(state.metaData().resolveIndexRouting(request.routing(), request.index()));
        request.index(state.metaData().concreteSingleIndex(request.index()));
        return true;
    }

    private void innerExecute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected DeleteRequest newRequestInstance() {
        return new DeleteRequest();
    }

    @Override
    protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
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
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, DeleteRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
    }

    @Override
    protected DeleteResponse shardOperationOnLeader(ClusterState clusterState, int replicaLevel, LeaderOperationRequest shardRequest) {
        final DeleteRequest request = shardRequest.request();
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request().index()).shardSafe(shardRequest.shardId());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        request.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        request.version(delete.version());

        assert request.versionType().validateVersionForWrites(request.version());

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_delete").force(false));
            } catch (Exception e) {
                // ignore
            }
        }

        int quorumShards = findQuorum(clusterState, shards(clusterState, request), request);

        return new DeleteResponse()
            .setIndex(request.index())
            .setType(request.type())
            .setId(request.id())
            .setVersion(delete.version())
            .setFound(delete.found())
            .setQuorumShards(quorumShards);
    }

}

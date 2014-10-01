package org.xbib.elasticsearch.action.index.leader;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.index.IndexAction;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.index.IndexResponse;
import org.xbib.elasticsearch.action.support.replication.leader.TransportLeaderShardOperationAction;

public class TransportLeaderShardIndexAction extends TransportLeaderShardOperationAction<IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final TransportCreateIndexAction createIndexAction;

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportLeaderShardIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                           TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction,
                                           ActionFilters actionFilters) {
        super(settings, IndexAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
        this.createIndexAction = createIndexAction;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
    }

    @Override
    protected void doExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            request.beforeLocalFork();
            createIndexAction.execute(new CreateIndexRequest(request.index()).cause("auto(index api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        try {
                            innerExecute(request, listener);
                        } catch (Throwable e1) {
                            listener.onFailure(e1);
                        }
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
    protected boolean resolveRequest(ClusterState state, IndexRequest request, ActionListener<IndexResponse> indexResponseActionListener) {
        MetaData metaData = clusterService.state().metaData();
        String aliasOrIndex = request.index();
        request.index(metaData.concreteSingleIndex(request.index(), request.indicesOptions()));
        MappingMetaData mappingMd = null;
        if (metaData.hasIndex(request.index())) {
            mappingMd = metaData.index(request.index()).mappingOrDefault(request.type());
        }
        request.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
        return true;
    }

    private void innerExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected IndexRequest newRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected String transportAction() {
        return IndexAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IndexRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
    }

    @Override
    protected IndexResponse shardOperationOnLeader(ClusterState clusterState, int replicaLevel, LeaderOperationRequest shardRequest) {
        final IndexRequest request = shardRequest.request();
        IndexMetaData indexMetaData = clusterState.metaData().index(request.index());
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (request.routing() == null) {
                throw new RoutingMissingException(request.index(), request.type(), request.id());
            }
        }
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request().index()).shardSafe(shardRequest.shardId());
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        long version;
        if (request.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse, request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, false);
            if (index.parsedDoc().mappingsModified()) {
                mappingUpdatedAction.updateMappingOnMaster(request.index(), index.docMapper(), indexMetaData.getUUID());
            }
            indexShard.index(index);
            version = index.version();
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse,
                    request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, false, request.autoGeneratedId());
            if (create.parsedDoc().mappingsModified()) {
                mappingUpdatedAction.updateMappingOnMaster(request.index(), create.docMapper(), indexMetaData.getUUID());
            }
            indexShard.create(create);
            version = create.version();
        }
        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_index").force(false));
            } catch (Exception e) {
                // ignore?
            }
        }
        request.version(version);
        request.versionType(request.versionType().versionTypeForReplicationAndRecovery());

        assert request.versionType().validateVersionForWrites(request.version());

        int quorumShards = findQuorum(clusterState, shards(clusterState, request), request);

        return new IndexResponse()
            .setIndex(request.index())
            .setType(request.type())
            .setId(request.id())
            .setVersion(version)
            .setQuorumShards(quorumShards);
    }

}


package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TransportShardIngestIndexAction extends TransportShardReplicationOperationAction<IngestIndexShardRequest, IngestIndexShardRequest, IngestIndexShardResponse> {

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardIngestIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                           MappingUpdatedAction mappingUpdatedAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return IngestIndexAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected IngestIndexShardRequest newRequestInstance() {
        return new IngestIndexShardRequest();
    }

    @Override
    protected IngestIndexShardRequest newReplicaRequestInstance() {
        return new IngestIndexShardRequest();
    }

    @Override
    protected IngestIndexShardResponse newResponseInstance() {
        return new IngestIndexShardResponse();
    }

    @Override
    protected String transportAction() {
        return IngestIndexAction.NAME + ".shard.index";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestIndexShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestIndexShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IngestIndexShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }

    @Override
    protected PrimaryResponse<IngestIndexShardResponse, IngestIndexShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final IngestIndexShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        Set<Tuple<String, String>> mappingsToUpdate = null;

        int successSize = 0;
        List<IngestItemFailure> failure = Lists.newLinkedList();
        int size = request.items().size();
        long[] versions = new long[size];
        for (int i = 0; i < size; i++) {
            IngestIndexItemRequest item = request.items().get(i);
            IndexRequest indexRequest = (IndexRequest) item.request();
            try {
                // validate, if routing is required, that we got routing
                MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
                if (mappingMd != null && mappingMd.routing().required()) {
                    if (indexRequest.routing() == null) {
                        throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
                    }
                }
                SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                        .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());
                long version;
                Engine.IndexingOperation op;
                if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                    Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.index(index);
                    version = index.version();
                    op = index;
                } else {
                    Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.create(create);
                    version = create.version();
                    op = create;
                }
                versions[i] = indexRequest.version();
                // update the version on request so it will happen on the replicas
                indexRequest.version(version);
                // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
                if (op.parsedDoc().mappingsModified()) {
                    if (mappingsToUpdate == null) {
                        mappingsToUpdate = Sets.newHashSet();
                    }
                    mappingsToUpdate.add(Tuple.tuple(indexRequest.index(), indexRequest.type()));
                }
                successSize++;
            } catch (Exception e) {
                // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                if (retryPrimaryException(e)) {
                    // restore updated versions...
                    for (int j = 0; j < i; j++) {
                        ((IndexRequest) request.items().get(j).request()).version(versions[j]);
                    }
                    throw (ElasticsearchException) e;
                }
                if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                    logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                } else {
                    logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                }
                failure.add(new IngestItemFailure(item.id(), ExceptionsHelper.detailedMessage(e)));
                // nullify the request so it won't execute on the replicas
                request.items().set(i, null);
            }
        }
        if (mappingsToUpdate != null) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2());
            }
        }
        IngestIndexShardResponse response = new IngestIndexShardResponse(new ShardId(request.index(), request.shardId()), successSize, failure);
        return new PrimaryResponse<IngestIndexShardResponse, IngestIndexShardRequest>(shardRequest.request, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        final IngestIndexShardRequest request = shardRequest.request;
        int size = request.items().size();
        for (int i = 0; i < size; i++) {
            IngestIndexItemRequest item = request.items().get(i);
            if (item == null) {
                continue;
            }
            IndexRequest indexRequest = (IndexRequest) item.request();
            try {
                SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                        .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

                if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                    Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.index(index);
                } else {
                    Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.create(create);
                }
            } catch (Exception e) {
                // ignore, we are on backup
            }
        }
    }

    private void updateMappingOnMaster(final String index, final String type) {
        try {
            MapperService mapperService = indicesService.indexServiceSafe(index).mapperService();
            final DocumentMapper documentMapper = mapperService.documentMapper(type);
            if (documentMapper == null) { // should not happen
                return;
            }
            IndexMetaData metaData = clusterService.state().metaData().index(index);
            if (metaData == null) {
                return;
            }
            long orderId = mappingUpdatedAction.generateNextMappingUpdateOrder();
            documentMapper.refreshSource();

            DiscoveryNode node = clusterService.localNode();
            final MappingUpdatedAction.MappingUpdatedRequest request = new MappingUpdatedAction.MappingUpdatedRequest(index, metaData.uuid(), type, documentMapper.mappingSource(), orderId, node != null ? node.id() : null);
            mappingUpdatedAction.execute(request, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override
                public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        logger.warn("failed to update master on updated mapping for index [{}], type [{}] and source [{}]", e, index, type, documentMapper.mappingSource().string());
                    } catch (IOException e1) {
                        // ignore
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("failed to update master on updated mapping for index [{}], type [{}]", e, index, type);
        }
    }

}


package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Sets.newHashSet;


public class TransportShardIngestAction extends TransportShardReplicationOperationAction<IngestShardRequest, IngestShardRequest, IngestShardResponse> {

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardIngestAction(Settings settings, TransportService transportService, ClusterService clusterService,
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
        return IngestAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected IngestShardRequest newRequestInstance() {
        return new IngestShardRequest();
    }

    @Override
    protected IngestShardRequest newReplicaRequestInstance() {
        return new IngestShardRequest();
    }

    @Override
    protected IngestShardResponse newResponseInstance() {
        return new IngestShardResponse();
    }

    @Override
    protected String transportAction() {
        return IngestAction.NAME + ".shard";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IngestShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }

    @Override
    protected PrimaryResponse<IngestShardResponse, IngestShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final IngestShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        Set<Tuple<String, String>> mappingsToUpdate = null;

        int successSize = 0;
        List<IngestItemFailure> failure = newLinkedList();
        int size = request.items().size();
        long[] versions = new long[size];
        for (int i = 0; i < size; i++) {
            IngestItemRequest item = request.items().get(i);
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    // validate, if routing is required, that we got routing
                    MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
                    if (mappingMd != null && mappingMd.routing().required()) {
                        if (indexRequest.routing() == null) {
                            throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
                        }
                    }
                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());
                    long version;
                    Engine.IndexingOperation op;
                    if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                        Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).versionType(indexRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
                        indexShard.index(index);
                        version = index.version();
                        op = index;
                    } else {
                        Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).versionType(indexRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
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
                            mappingsToUpdate = newHashSet();
                        }
                        mappingsToUpdate.add(Tuple.tuple(indexRequest.index(), indexRequest.type()));
                    }

                    successSize++;

                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < i; j++) {
                            applyVersion(request.items().get(j), versions[j]);
                        }
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    }
                    failure.add(new IngestItemFailure(item.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items().set(i, null);
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).versionType(deleteRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
                    indexShard.delete(delete);
                    // update the request with teh version so it will go to the replicas
                    deleteRequest.version(delete.version());

                    successSize++;
                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < i; j++) {
                            applyVersion(request.items().get(j), versions[j]);
                        }
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    }
                    failure.add(new IngestItemFailure(item.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items().set(i, null);
                }
            }
        }

        if (mappingsToUpdate != null) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2());
            }
        }

        IngestShardResponse response = new IngestShardResponse(new ShardId(request.index(), request.shardId()), successSize, failure);
        return new PrimaryResponse<IngestShardResponse, IngestShardRequest>(shardRequest.request, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        final IngestShardRequest request = shardRequest.request;
        int size = request.items().size();
        for (int i = 0; i < size; i++) {
            IngestItemRequest item = request.items().get(i);
            if (item == null) {
                continue;
            }
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
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
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.delete(delete);
                } catch (Exception e) {
                    // ignore, we are on backup
                }
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
            documentMapper.refreshSource();

            final MappingUpdatedAction.MappingUpdatedRequest request = new MappingUpdatedAction.MappingUpdatedRequest(index, type, documentMapper.mappingSource());
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

    private void applyVersion(IngestItemRequest item, long version) {
        if (item.request() instanceof IndexRequest) {
            ((IndexRequest) item.request()).version(version);
        } else if (item.request() instanceof DeleteRequest) {
            ((DeleteRequest) item.request()).version(version);
        }
    }
}

package org.xbib.elasticsearch.action.ingest.leader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RoutingMissingException;
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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;
import org.xbib.elasticsearch.action.support.replication.leader.TransportLeaderShardOperationAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Sets.newHashSet;

public class TransportLeaderShardIngestAction extends TransportLeaderShardOperationAction<IngestLeaderShardRequest, IngestLeaderShardResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportLeaderShardIngestAction.class.getSimpleName());

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportLeaderShardIngestAction(Settings settings, TransportService transportService, ClusterService clusterService,
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
    protected TransportRequestOptions transportOptions() {
        return IngestAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected IngestLeaderShardRequest newRequestInstance() {
        return new IngestLeaderShardRequest();
    }

    @Override
    protected IngestLeaderShardResponse newResponseInstance() {
        return new IngestLeaderShardResponse();
    }

    @Override
    protected String transportAction() {
        return IngestAction.NAME + ".shard.leader";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestLeaderShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestLeaderShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IngestLeaderShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.getShardId().id()).shardsIt();
    }

    @Override
    protected IngestLeaderShardResponse shardOperationOnLeader(ClusterState clusterState, int replicaLevel, LeaderOperationRequest shardRequest) {
        final long t0 = shardRequest.startTime();
        final IngestLeaderShardRequest request = shardRequest.request();
        int successCount = 0;
        List<IngestActionFailure> failures = newLinkedList();
        int size = request.getActionRequests().size();
        long[] versions = new long[size];
        Set<Tuple<String, String>> mappingsToUpdate = newHashSet();
        for (int i = 0; i < size; i++) {
            ActionRequest actionRequest = request.getActionRequests().get(i);
            if (actionRequest instanceof IndexRequest) {
                try {
                    IndexRequest indexRequest = (IndexRequest) actionRequest;
                    long version = indexOperationOnLeader(clusterState, indexRequest, request, mappingsToUpdate);
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
                    IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(shardRequest.shardId());
                    DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version(), deleteRequest.versionType(), Engine.Operation.Origin.PRIMARY);
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
        if (!mappingsToUpdate.isEmpty()) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                mappingUpdatedAction.updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2(), true);
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

    private long indexOperationOnLeader(ClusterState clusterState, IndexRequest indexRequest,
                                        IngestLeaderShardRequest request, Set<Tuple<String, String>> mappingsToUpdate) {
        boolean mappingsModified = false;
        try {
            MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
            if (mappingMd != null && mappingMd.routing().required() && indexRequest.routing() == null) {
                throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
            SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, indexRequest.source())
                    .type(indexRequest.type())
                    .id(indexRequest.id())
                    .routing(indexRequest.routing())
                    .parent(indexRequest.parent())
                    .timestamp(indexRequest.timestamp())
                    .ttl(indexRequest.ttl());
            IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.getShardId().id());
            if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                Engine.Index index = indexShard.prepareIndex(sourceToParse,
                        indexRequest.version(),
                        indexRequest.versionType(),
                        Engine.Operation.Origin.PRIMARY,
                        false);
                mappingsModified = index.parsedDoc().mappingsModified();
                indexShard.index(index);
                return index.version();
            } else if (indexRequest.opType() == IndexRequest.OpType.CREATE) {
                Engine.Create create = indexShard.prepareCreate(sourceToParse,
                        indexRequest.version(),
                        indexRequest.versionType(),
                        Engine.Operation.Origin.PRIMARY,
                        false,
                        indexRequest.autoGeneratedId());
                mappingsModified = create.parsedDoc().mappingsModified();
                indexShard.create(create);
                return create.version();
            } else {
                logger.error("unknown op type " + indexRequest.opType());
                return 0;
            }
        } finally {
            if (mappingsModified) {
                mappingsToUpdate.add(Tuple.tuple(indexRequest.index(), indexRequest.type()));
            }
        }
    }

}

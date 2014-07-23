package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
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
import org.xbib.elasticsearch.action.support.replication.replica.TransportReplicaShardOperationAction;

import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class TransportReplicaShardIngestAction
        extends TransportReplicaShardOperationAction<IngestReplicaShardRequest, IngestReplicaShardResponse, TransportReplicaShardOperationAction.ReplicaOperationResponse> {

    @Inject
    public TransportReplicaShardIngestAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings, IngestAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction);
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
    protected IngestReplicaShardRequest newRequestInstance() {
        return new IngestReplicaShardRequest();
    }

    @Override
    protected IngestReplicaShardResponse newResponseInstance() {
        return new IngestReplicaShardResponse();
    }

    @Override
    protected String transportAction() {
        return IngestAction.NAME + ".shard.replica";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestReplicaShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestReplicaShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IngestReplicaShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId().id()).shardsIt();
    }

    @Override
    protected ReplicaOperationResponse newReplicaResponseInstance() {
        return new ReplicaOperationResponse();
    }

    @Override
    protected IngestReplicaShardResponse shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        final long t0 = shardRequest.startTime();
        final IngestReplicaShardRequest request = shardRequest.request();
        final IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request().index()).shardSafe(shardRequest.shardId());
        int successCount = 0;
        List<IngestActionFailure> failure = newLinkedList();
        int size = request.actionRequests().size();
        for (int i = 0; i < size; i++) {
            ActionRequest actionRequest = request.actionRequests().get(i);
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
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(),
                            deleteRequest.id(),
                            deleteRequest.version(),
                            deleteRequest.versionType(),
                            Engine.Operation.Origin.REPLICA);
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
            Engine.Index index = indexShard.prepareIndex(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    Engine.Operation.Origin.REPLICA,
                    false);
            indexShard.index(index);
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse,
                    indexRequest.version(),
                    indexRequest.versionType(),
                    Engine.Operation.Origin.REPLICA,
                    false,
                    indexRequest.autoGeneratedId());
            indexShard.create(create);
        }
    }

}

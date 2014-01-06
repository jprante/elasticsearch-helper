
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;

import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;


public class TransportShardIngestDeleteAction extends TransportShardReplicationOperationAction<IngestDeleteShardRequest, IngestDeleteShardRequest, IngestDeleteShardResponse> {

    @Inject
    public TransportShardIngestDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
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
        return IngestDeleteAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected IngestDeleteShardRequest newRequestInstance() {
        return new IngestDeleteShardRequest();
    }

    @Override
    protected IngestDeleteShardRequest newReplicaRequestInstance() {
        return new IngestDeleteShardRequest();
    }

    @Override
    protected IngestDeleteShardResponse newResponseInstance() {
        return new IngestDeleteShardResponse();
    }

    @Override
    protected String transportAction() {
        return IngestDeleteAction.NAME + ".shard.delete";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IngestDeleteShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IngestDeleteShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IngestDeleteShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }

    @Override
    protected PrimaryResponse<IngestDeleteShardResponse, IngestDeleteShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final IngestDeleteShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);

        int successSize = 0;
        List<IngestItemFailure> failure = newLinkedList();
        int size = request.items().size();
        long[] versions = new long[size];
        for (int i = 0; i < size; i++) {
            IngestDeleteItemRequest item = request.items().get(i);
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).versionType(deleteRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
                    indexShard.delete(delete);
                    versions[i] = delete.version();
                    // update the request with the version so it will go to the replicas
                    deleteRequest.version(delete.version());
                    successSize++;
                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < i; j++) {
                            ((DeleteRequest)request.items().get(j).request()).version(versions[j]);
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

        IngestDeleteShardResponse response = new IngestDeleteShardResponse(new ShardId(request.index(), request.shardId()), successSize, failure);
        return new PrimaryResponse<IngestDeleteShardResponse, IngestDeleteShardRequest>(shardRequest.request, response, null);
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        final IngestDeleteShardRequest request = shardRequest.request;
        int size = request.items().size();
        for (int i = 0; i < size; i++) {
            IngestDeleteItemRequest item = request.items().get(i);
            if (item == null) {
                continue;
            }
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

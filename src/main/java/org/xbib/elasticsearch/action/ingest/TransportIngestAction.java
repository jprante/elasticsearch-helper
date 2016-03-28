package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.ingest.leader.IngestLeaderShardRequest;
import org.xbib.elasticsearch.action.ingest.leader.IngestLeaderShardResponse;
import org.xbib.elasticsearch.action.ingest.leader.TransportLeaderShardIngestAction;
import org.xbib.elasticsearch.action.ingest.replica.IngestReplicaShardRequest;
import org.xbib.elasticsearch.action.ingest.replica.TransportReplicaShardIngestAction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportIngestAction extends HandledTransportAction<IngestRequest, IngestResponse> {

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportLeaderShardIngestAction leaderShardIngestAction;

    private final TransportReplicaShardIngestAction replicaShardIngestAction;

    @Inject
    public TransportIngestAction(Settings settings, ThreadPool threadPool,
                                 TransportService transportService, ClusterService clusterService,
                                 TransportLeaderShardIngestAction leaderShardIngestAction,
                                 TransportReplicaShardIngestAction replicaShardIngestAction,
                                 ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IngestAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, IngestRequest.class);
        this.clusterService = clusterService;
        this.leaderShardIngestAction = leaderShardIngestAction;
        this.replicaShardIngestAction = replicaShardIngestAction;
        this.allowIdGeneration = this.settings.getAsBoolean("action.allow_id_generation", true);
    }

    @Override
    protected void doExecute(final IngestRequest ingestRequest, final ActionListener<IngestResponse> listener) {
        final long startTime = System.currentTimeMillis();
        final IngestResponse ingestResponse = new IngestResponse();
        ingestResponse.setIngestId(ingestRequest.ingestId());
        ClusterState clusterState = clusterService.state();
        try {
            clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);
        } catch (ClusterBlockException e) {
            listener.onFailure(e);
            return;
        }
        final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
        MetaData metaData = clusterState.metaData();
        final List<ActionRequest<?>> requests = new LinkedList<>();
        for (ActionRequest<?> request : ingestRequest.requests()) {
            String concreteIndex = concreteIndices.resolveIfAbsent((DocumentRequest)request);
            if (request instanceof IndexRequest) {
                try {
                    IndexRequest indexRequest = (IndexRequest) request;
                    indexRequest.routing(metaData.resolveIndexRouting(indexRequest.routing(), concreteIndex));
                    indexRequest.index(concreteIndex);
                    MappingMetaData mappingMd = null;
                    if (metaData.hasIndex(concreteIndex)) {
                        mappingMd = metaData.index(concreteIndex).mappingOrDefault(indexRequest.type());
                    }
                    indexRequest.process(metaData, mappingMd, allowIdGeneration, concreteIndex);
                    requests.add(indexRequest);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    ingestResponse.addFailure(new IngestActionFailure(-1L, null, ExceptionsHelper.detailedMessage(e)));
                }
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                deleteRequest.routing(metaData.resolveIndexRouting(deleteRequest.routing(), concreteIndex));
                deleteRequest.index(concreteIndex);
                requests.add(deleteRequest);
            } else {
                throw new ElasticsearchException("action request not known: " + request.getClass().getName());
            }
        }
        // second, go over all the requests and create a shard request map
        Map<ShardId, List<ActionRequest<?>>> requestsByShard = new HashMap<>();
        for (ActionRequest<?> request : requests) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(indexRequest.index());
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<ActionRequest<?>> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = new LinkedList<>();
                    requestsByShard.put(shardId, list);
                }
                list.add(request);
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(deleteRequest.index());
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                List<ActionRequest<?>> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = new LinkedList<>();
                    requestsByShard.put(shardId, list);
                }
                list.add(deleteRequest);
            }
        }
        if (requestsByShard.isEmpty()) {
            logger.error("no shards to execute ingest");
            ingestResponse.setSuccessSize(0)
                    .addFailure(new IngestActionFailure(-1L, null, "no shards to execute ingest"))
                    .setTookInMillis(System.currentTimeMillis() - startTime);
            listener.onResponse(ingestResponse);
            return;
        }
        // third, for each shard, execute leader/replica action
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger responseCounter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<ActionRequest<?>>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<ActionRequest<?>> actionRequests = entry.getValue();
            final IngestLeaderShardRequest ingestLeaderShardRequest = new IngestLeaderShardRequest()
                    .setIngestId(ingestRequest.ingestId())
                    .setShardId(shardId)
                    .setActionRequests(actionRequests)
                    .timeout(ingestRequest.timeout())
                    .requiredConsistency(ingestRequest.requiredConsistency());
            leaderShardIngestAction.execute(ingestLeaderShardRequest, new ActionListener<IngestLeaderShardResponse>() {
                @Override
                public void onResponse(IngestLeaderShardResponse ingestLeaderShardResponse) {
                    long millis = System.currentTimeMillis() - startTime;
                    ingestResponse.setIngestId(ingestRequest.ingestId());
                    ingestResponse.setLeaderResponse(ingestLeaderShardResponse);
                    successCount.addAndGet(ingestLeaderShardResponse.getSuccessCount());
                    int quorumShards = ingestLeaderShardResponse.getQuorumShards();
                    if (quorumShards < 0) {
                        ingestResponse.addFailure(new IngestActionFailure(ingestRequest.ingestId(), shardId, "quorum not reached for shard " + shardId));
                    } else if (quorumShards > 0) {
                        responseCounter.incrementAndGet();
                        final IngestReplicaShardRequest ingestReplicaShardRequest =
                                new IngestReplicaShardRequest(ingestLeaderShardRequest.getIngestId(),
                                        ingestLeaderShardRequest.getShardId(),
                                        ingestLeaderShardRequest.getActionRequests());
                        ingestReplicaShardRequest.timeout(ingestRequest.timeout());
                        replicaShardIngestAction.execute(ingestReplicaShardRequest, new ActionListener<TransportReplicaShardIngestAction.ReplicaOperationResponse>() {
                            @Override
                            public void onResponse(TransportReplicaShardIngestAction.ReplicaOperationResponse response) {
                                long millis = Math.max(1, System.currentTimeMillis() - startTime);
                                ingestResponse.addReplicaResponses(response.responses());
                                if (responseCounter.decrementAndGet() == 0) {
                                    ingestResponse.setSuccessSize(successCount.get())
                                            .setTookInMillis(millis);
                                    listener.onResponse(ingestResponse);
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                long millis = Math.max(1, System.currentTimeMillis() - startTime);
                                logger.error(e.getMessage(), e);
                                ingestResponse.addFailure(new IngestActionFailure(ingestRequest.ingestId(), shardId, ExceptionsHelper.detailedMessage(e)));
                                if (responseCounter.decrementAndGet() == 0) {
                                    ingestResponse.setSuccessSize(successCount.get())
                                            .setTookInMillis(millis);
                                    listener.onResponse(ingestResponse);
                                }
                            }
                        });
                    }
                    if (responseCounter.decrementAndGet() == 0) {
                        ingestResponse.setSuccessSize(successCount.get()).setTookInMillis(millis);
                        listener.onResponse(ingestResponse);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    long millis = System.currentTimeMillis() - startTime;
                    logger.error(e.getMessage(), e);
                    ingestResponse.addFailure(new IngestActionFailure(-1L, shardId, ExceptionsHelper.detailedMessage(e)));
                    if (responseCounter.decrementAndGet() == 0) {
                        ingestResponse.setSuccessSize(successCount.get()).setTookInMillis(millis);
                        listener.onResponse(ingestResponse);
                    }
                }

            });
        }
    }

    private static class ConcreteIndices  {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, String> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        String getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        String resolveIfAbsent(DocumentRequest<?> request) {
            String concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                concreteIndex = indexNameExpressionResolver.concreteSingleIndex(state, request);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }

}

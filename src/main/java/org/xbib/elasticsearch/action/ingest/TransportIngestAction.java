package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.ingest.leader.IngestLeaderShardRequest;
import org.xbib.elasticsearch.action.ingest.leader.IngestLeaderShardResponse;
import org.xbib.elasticsearch.action.ingest.leader.TransportLeaderShardIngestAction;
import org.xbib.elasticsearch.action.ingest.replica.IngestReplicaShardRequest;
import org.xbib.elasticsearch.action.ingest.replica.TransportReplicaShardIngestAction;
import org.xbib.elasticsearch.action.support.replication.replica.TransportReplicaShardOperationAction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

public class TransportIngestAction extends TransportAction<IngestRequest, IngestResponse> {

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportLeaderShardIngestAction leaderShardIngestAction;

    private final TransportReplicaShardIngestAction replicaShardIngestAction;

    @Inject
    public TransportIngestAction(Settings settings, ThreadPool threadPool,
                                 TransportService transportService, ClusterService clusterService,
                                 TransportLeaderShardIngestAction leaderShardIngestAction,
                                 TransportReplicaShardIngestAction replicaShardIngestAction,
                                 ActionFilters actionFilter) {
        super(settings, IngestAction.NAME, threadPool, actionFilter);
        this.clusterService = clusterService;
        this.leaderShardIngestAction = leaderShardIngestAction;
        this.replicaShardIngestAction = replicaShardIngestAction;
        this.allowIdGeneration = componentSettings.getAsBoolean("action.allow_id_generation", true);

        transportService.registerHandler(IngestAction.NAME, new IngestTransportHandler());
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
        MetaData metaData = clusterState.metaData();
        final List<ActionRequest> requests = newLinkedList();
        // first, iterate over all requests and parse them for mapping, filter out erraneous requests
        for (ActionRequest request : ingestRequest.requests()) {
            if (request instanceof IndexRequest) {
                try {
                    IndexRequest indexRequest = (IndexRequest) request;
                    indexRequest.index(clusterState.metaData().concreteSingleIndex(indexRequest.index(), indexRequest.indicesOptions()));
                    MappingMetaData mappingMd = null;
                    if (metaData.hasIndex(indexRequest.index())) {
                        mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
                    }
                    indexRequest.process(metaData, indexRequest.index(), mappingMd, allowIdGeneration);
                    requests.add(indexRequest);
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    ingestResponse.addFailure(new IngestActionFailure(-1L, null, ExceptionsHelper.detailedMessage(e)));
                }
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                deleteRequest.routing(clusterState.metaData().resolveIndexRouting(deleteRequest.routing(), deleteRequest.index()));
                deleteRequest.index(clusterState.metaData().concreteSingleIndex(deleteRequest.index(), deleteRequest.indicesOptions()));
                requests.add(deleteRequest);
            } else {
                throw new ElasticsearchIllegalArgumentException("action request not known: " + request.getClass().getName());
            }
        }
        // second, go over all the requests and create a shard request map
        Map<ShardId, List<ActionRequest>> requestsByShard = newHashMap();
        for (ActionRequest request : requests) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<ActionRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = newLinkedList();
                    requestsByShard.put(shardId, list);
                }
                list.add(request);
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                MappingMetaData mappingMd = clusterState.metaData().index(deleteRequest.index()).mappingOrDefault(deleteRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && deleteRequest.routing() == null) {
                    GroupShardsIterator groupShards = clusterService.operationRouting().broadcastDeleteShards(clusterState, deleteRequest.index());
                    for (ShardIterator shardIt : groupShards) {
                        List<ActionRequest> list = requestsByShard.get(shardIt.shardId());
                        if (list == null) {
                            list = newLinkedList();
                            requestsByShard.put(shardIt.shardId(), list);
                        }
                        list.add(deleteRequest);
                    }
                } else {
                    ShardId shardId = clusterService.operationRouting().deleteShards(clusterState, deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                    List<ActionRequest> list = requestsByShard.get(shardId);
                    if (list == null) {
                        list = newLinkedList();
                        requestsByShard.put(shardId, list);
                    }
                    list.add(request);
                }
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
        for (Map.Entry<ShardId, List<ActionRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<ActionRequest> actionRequests = entry.getValue();
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
                        replicaShardIngestAction.execute(ingestReplicaShardRequest, new ActionListener<TransportReplicaShardOperationAction.ReplicaOperationResponse>() {
                            @Override
                            public void onResponse(TransportReplicaShardOperationAction.ReplicaOperationResponse response) {
                                long millis = System.currentTimeMillis() - startTime;
                                ingestResponse.addReplicaResponses(response.responses());
                                if (responseCounter.decrementAndGet() == 0) {
                                    ingestResponse.setSuccessSize(successCount.get())
                                            .setTookInMillis(millis);
                                    listener.onResponse(ingestResponse);
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                long millis = System.currentTimeMillis() - startTime;
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
                        ingestResponse.setSuccessSize(successCount.get())
                                .setTookInMillis(millis);
                        listener.onResponse(ingestResponse);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    long millis = System.currentTimeMillis() - startTime;
                    logger.error(e.getMessage(), e);
                    ingestResponse.addFailure(new IngestActionFailure(-1L, shardId, ExceptionsHelper.detailedMessage(e)));
                    if (responseCounter.decrementAndGet() == 0) {
                        ingestResponse.setSuccessSize(successCount.get())
                                .setTookInMillis(millis);
                        listener.onResponse(ingestResponse);
                    }
                }

            });
        }
    }

    class IngestTransportHandler extends BaseTransportRequestHandler<IngestRequest> {

        @Override
        public IngestRequest newInstance() {
            return new IngestRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final IngestRequest request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            request.operationThreaded(true);
            execute(request, new ActionListener<IngestResponse>() {
                @Override
                public void onResponse(IngestResponse result) {
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
                        logger.warn("failed to send error response for action [" + IngestAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }
    }
}


package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

public class TransportIngestIndexAction extends TransportAction<IngestIndexRequest, IngestResponse> {

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportShardIngestIndexAction shardBulkAction;

    @Inject
    public TransportIngestIndexAction(Settings settings,
                                      ThreadPool threadPool,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      TransportShardIngestIndexAction shardBulkAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;

        this.allowIdGeneration = componentSettings.getAsBoolean("action.allow_id_generation", true);

        transportService.registerHandler(IngestIndexAction.NAME, new IngestTransportHandler());
    }

    @Override
    protected void doExecute(final IngestIndexRequest ingestRequest, final ActionListener<IngestResponse> listener) {
        executeBulk(ingestRequest, listener);
    }

    private void executeBulk(final IngestIndexRequest ingestRequest, final ActionListener<IngestResponse> listener) {
        final long startTime = System.currentTimeMillis();
        ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        MetaData metaData = clusterState.metaData();
        for (ActionRequest request : ingestRequest.requests()) {
            IndexRequest indexRequest = (IndexRequest) request;
            String aliasOrIndex = indexRequest.index();
            indexRequest.index(clusterState.metaData().concreteIndex(indexRequest.index()));
            MappingMetaData mappingMd = null;
            if (metaData.hasIndex(indexRequest.index())) {
                mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
            }
            indexRequest.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        Map<ShardId, List<IngestIndexItemRequest>> requestsByShard = newHashMap();
        int i = 0;
        for (ActionRequest request : ingestRequest.requests()) {
            IndexRequest indexRequest = (IndexRequest) request;
            ShardId shardId = clusterService.operationRouting().indexShards(clusterState, indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
            List<IngestIndexItemRequest> list = requestsByShard.get(shardId);
            if (list == null) {
                list = newLinkedList();
                requestsByShard.put(shardId, list);
            }
            list.add(new IngestIndexItemRequest(i, request));
            i++;
        }

        final AtomicInteger successSize = new AtomicInteger(0);
        final List<IngestItemFailure> failure = newLinkedList();

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new IngestResponse(0, failure, System.currentTimeMillis() - startTime));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<IngestIndexItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<IngestIndexItemRequest> requests = entry.getValue();
            IngestIndexShardRequest ingestShardRequest = new IngestIndexShardRequest(shardId.index().name(), shardId.id(), requests);
            ingestShardRequest.replicationType(ingestRequest.replicationType());
            ingestShardRequest.consistencyLevel(ingestRequest.consistencyLevel());
            ingestShardRequest.timeout(ingestRequest.timeout());
            shardBulkAction.execute(ingestShardRequest, new ActionListener<IngestIndexShardResponse>() {
                @Override
                public void onResponse(IngestIndexShardResponse ingestShardResponse) {
                    successSize.addAndGet(ingestShardResponse.successSize());
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // create failures for all relevant requests
                    String message = ExceptionsHelper.detailedMessage(e);
                    synchronized (failure) {
                        for (IngestIndexItemRequest request : requests) {
                            failure.add(new IngestItemFailure(request.id(), message));
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new IngestResponse(successSize.get(), failure, System.currentTimeMillis() - startTime));
                }
            });
        }
    }

    class IngestTransportHandler extends BaseTransportRequestHandler<IngestIndexRequest> {

        @Override
        public IngestIndexRequest newInstance() {
            return new IngestIndexRequest();
        }

        @Override
        public void messageReceived(final IngestIndexRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<IngestResponse>() {
                @Override
                public void onResponse(IngestResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

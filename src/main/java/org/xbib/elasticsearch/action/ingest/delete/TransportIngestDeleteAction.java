
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
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

public class TransportIngestDeleteAction extends TransportAction<IngestDeleteRequest, IngestResponse> {

    private final ClusterService clusterService;

    private final TransportShardIngestDeleteAction shardBulkAction;

    @Inject
    public TransportIngestDeleteAction(Settings settings, ThreadPool threadPool,
                                       TransportService transportService,
                                       ClusterService clusterService,
                                       TransportShardIngestDeleteAction shardBulkAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;

        transportService.registerHandler(IngestDeleteAction.NAME, new IngestTransportHandler());
    }

    @Override
    protected void doExecute(final IngestDeleteRequest ingestRequest, final ActionListener<IngestResponse> listener) {
        final long startTime = System.currentTimeMillis();
        executeBulk(ingestRequest, startTime, listener);
    }

    private void executeBulk(final IngestDeleteRequest ingestRequest, final long startTime, final ActionListener<IngestResponse> listener) {
        ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        for (ActionRequest request : ingestRequest.requests()) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            deleteRequest.routing(clusterState.metaData().resolveIndexRouting(deleteRequest.routing(), deleteRequest.index()));
            deleteRequest.index(clusterState.metaData().concreteIndex(deleteRequest.index()));
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        Map<ShardId, List<IngestDeleteItemRequest>> requestsByShard = newHashMap();
        int i = 0;
        for (ActionRequest request : ingestRequest.requests()) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            MappingMetaData mappingMd = clusterState.metaData().index(deleteRequest.index()).mappingOrDefault(deleteRequest.type());
            if (mappingMd != null && mappingMd.routing().required() && deleteRequest.routing() == null) {
                // if routing is required, and no routing on the delete request, we need to broadcast it....
                GroupShardsIterator groupShards = clusterService.operationRouting().broadcastDeleteShards(clusterState, deleteRequest.index());
                for (ShardIterator shardIt : groupShards) {
                    List<IngestDeleteItemRequest> list = requestsByShard.get(shardIt.shardId());
                    if (list == null) {
                        list = newLinkedList();
                        requestsByShard.put(shardIt.shardId(), list);
                    }
                    list.add(new IngestDeleteItemRequest(i, deleteRequest));
                }
            } else {
                ShardId shardId = clusterService.operationRouting().deleteShards(clusterState, deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                List<IngestDeleteItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = newLinkedList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new IngestDeleteItemRequest(i, request));
            }
            i++;
        }

        final AtomicInteger successSize = new AtomicInteger(0);
        final List<IngestItemFailure> failure = newLinkedList();

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new IngestResponse(0, failure, System.currentTimeMillis() - startTime));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<IngestDeleteItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<IngestDeleteItemRequest> requests = entry.getValue();
            IngestDeleteShardRequest ingestShardRequest = new IngestDeleteShardRequest(shardId.index().name(), shardId.id(), requests);
            ingestShardRequest.replicationType(ingestRequest.replicationType());
            ingestShardRequest.consistencyLevel(ingestRequest.consistencyLevel());
            shardBulkAction.execute(ingestShardRequest, new ActionListener<IngestDeleteShardResponse>() {
                @Override
                public void onResponse(IngestDeleteShardResponse ingestShardResponse) {
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
                        for (IngestDeleteItemRequest request : requests) {
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

    class IngestTransportHandler extends BaseTransportRequestHandler<IngestDeleteRequest> {

        @Override
        public IngestDeleteRequest newInstance() {
            return new IngestDeleteRequest();
        }

        @Override
        public void messageReceived(final IngestDeleteRequest request, final TransportChannel channel) throws Exception {
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

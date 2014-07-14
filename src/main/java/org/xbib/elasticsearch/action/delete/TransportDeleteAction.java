package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.action.delete.leader.TransportLeaderShardDeleteAction;
import org.xbib.elasticsearch.action.delete.replica.DeleteReplicaShardRequest;
import org.xbib.elasticsearch.action.delete.replica.TransportReplicaShardDeleteAction;
import org.xbib.elasticsearch.action.support.replication.replica.TransportReplicaShardOperationAction;

import java.util.concurrent.atomic.AtomicInteger;

public class TransportDeleteAction extends TransportAction<DeleteRequest, DeleteResponse> {

    private final ClusterService clusterService;

    private final TransportLeaderShardDeleteAction transportLeaderShardDeleteAction;

    private final TransportReplicaShardDeleteAction transportReplicaShardDeleteAction;

    @Inject
    public TransportDeleteAction(Settings settings, ThreadPool threadPool,
                                 TransportService transportService, ClusterService clusterService,
                                 TransportLeaderShardDeleteAction transportLeaderShardDeleteAction,
                                 TransportReplicaShardDeleteAction transportReplicaShardDeleteAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.transportLeaderShardDeleteAction = transportLeaderShardDeleteAction;
        this.transportReplicaShardDeleteAction = transportReplicaShardDeleteAction;

        transportService.registerHandler(DeleteAction.NAME, new DeleteTransportHandler());
    }

    @Override
    protected void doExecute(final DeleteRequest deleteRequest, final ActionListener<DeleteResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);
        final AtomicInteger counter = new AtomicInteger(1);
        final DeleteResponse response = new DeleteResponse();
        transportLeaderShardDeleteAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                int quorumShards = deleteResponse.getQuorumShards();
                response.setIndex(deleteRequest.index())
                        .setType(deleteRequest.type())
                        .setId(deleteRequest.id())
                        .setVersion(deleteResponse.getVersion())
                        .setFound(deleteResponse.isFound())
                        .setQuorumShards(quorumShards);
                ShardId shardId = clusterService.operationRouting().indexShards(clusterService.state(),
                        deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                if (quorumShards < 0) {
                    deleteResponse.setFailure(new DeleteActionFailure(shardId, "quorum not reached"));
                } else if (quorumShards > 0) {
                    counter.incrementAndGet();
                    DeleteReplicaShardRequest deleteReplicaShardRequest = new DeleteReplicaShardRequest(shardId, deleteRequest);
                    transportReplicaShardDeleteAction.execute(deleteReplicaShardRequest, new ActionListener<TransportReplicaShardOperationAction.ReplicaOperationResponse>() {
                        @Override
                        public void onResponse(TransportReplicaShardOperationAction.ReplicaOperationResponse replicaOperationResponse) {
                            response.addReplicaResponses(replicaOperationResponse.responses());
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(response);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error(e.getMessage(), e);
                            listener.onFailure(e);
                        }
                    });
                }
                if (counter.decrementAndGet() == 0) {
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error(e.getMessage(), e);
                listener.onFailure(e);
            }
        });
    }

    class DeleteTransportHandler extends BaseTransportRequestHandler<DeleteRequest> {

        @Override
        public DeleteRequest newInstance() {
            return new DeleteRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(final DeleteRequest request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            request.operationThreaded(true);
            execute(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse result) {
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
                        logger.warn("failed to send error response for action [" + DeleteAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }
    }
}

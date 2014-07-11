package org.xbib.elasticsearch.action.delete.replica;

import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.support.replication.replica.ReplicaShardOperationRequest;

public class DeleteReplicaShardRequest extends ReplicaShardOperationRequest<DeleteReplicaShardRequest> {

    private ShardId shardId;

    private DeleteRequest deleteRequest;

    public DeleteReplicaShardRequest() {
    }

    public DeleteReplicaShardRequest(ShardId shardId, DeleteRequest deleteRequest) {
        this.index = shardId.index().name();
        this.shardId = shardId;
        this.deleteRequest = deleteRequest;
    }

    public ShardId shardId() {
        return shardId;
    }

    public DeleteRequest request() {
        return deleteRequest;
    }

}

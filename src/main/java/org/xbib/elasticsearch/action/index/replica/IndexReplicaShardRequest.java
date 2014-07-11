package org.xbib.elasticsearch.action.index.replica;

import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.support.replication.replica.ReplicaShardOperationRequest;

public class IndexReplicaShardRequest extends ReplicaShardOperationRequest<IndexReplicaShardRequest> {

    private ShardId shardId;

    private IndexRequest indexRequest;

    public IndexReplicaShardRequest() {
    }

    public IndexReplicaShardRequest(ShardId shardId, IndexRequest indexRequest) {
        this.index = shardId.index().name();
        this.shardId = shardId;
        this.indexRequest = indexRequest;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexRequest request() {
        return indexRequest;
    }

}

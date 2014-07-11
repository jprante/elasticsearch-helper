package org.xbib.elasticsearch.action.index.replica;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class IndexReplicaShardResponse extends ActionResponse {

    protected ShardId shardId;

    protected int replicaLevel;

    protected long tookInMillis;

    public IndexReplicaShardResponse() {
    }

    public IndexReplicaShardResponse(ShardId shardId, int replicaLevel, long tookInMillis) {
        this.shardId = shardId;
        this.replicaLevel = replicaLevel;
        this.tookInMillis = tookInMillis;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int replicaLevel() {
        return replicaLevel;
    }

    public long tookInMillis() {
        return tookInMillis;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        replicaLevel = in.readVInt();
        tookInMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(replicaLevel);
        out.writeLong(tookInMillis);
    }

    public String toString() {
        return "shardId=" + shardId
                + ",replicaLevel=" + replicaLevel
                + ",tookInMillis=" + tookInMillis;
    }
}
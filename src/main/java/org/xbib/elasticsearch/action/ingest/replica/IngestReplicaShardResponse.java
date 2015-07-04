package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class IngestReplicaShardResponse extends ActionResponse {

    protected long ingestId;

    protected ShardId shardId;

    protected int replicaLevel;

    protected int successSize;

    protected long tookInMillis;

    protected List<IngestActionFailure> failures = new LinkedList<>();

    public IngestReplicaShardResponse() {
    }

    public IngestReplicaShardResponse(long ingestId, ShardId shardId, int replicaLevel, int successSize, long tookInMillis, List<IngestActionFailure> failures) {
        this.ingestId = ingestId;
        this.shardId = shardId;
        this.replicaLevel = replicaLevel;
        this.successSize = successSize;
        this.tookInMillis = tookInMillis;
        this.failures = failures;
    }

    public long ingestId() {
        return ingestId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int replicaLevel() {
        return replicaLevel;
    }

    public int getSuccessSize() {
        return successSize;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public List<IngestActionFailure> getFailures() {
        return failures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        ingestId = in.readLong();
        shardId = ShardId.readShardId(in);
        replicaLevel = in.readVInt();
        successSize = in.readVInt();
        tookInMillis = in.readLong();
        failures = new LinkedList<>();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            failures.add(IngestActionFailure.from(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(ingestId);
        shardId.writeTo(out);
        out.writeVInt(replicaLevel);
        out.writeVInt(successSize);
        out.writeLong(tookInMillis);
        out.writeVInt(failures.size());
        for (IngestActionFailure f : failures) {
            f.writeTo(out);
        }
    }

    public String toString() {
        return "ingestId=" + ingestId
                + ",shardId=" + shardId
                + ",replicaLevel=" + replicaLevel
                + ",successSize=" + successSize
                + ",tookInMillis=" + tookInMillis
                + ",failures=" + failures;
    }
}
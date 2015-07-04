package org.xbib.elasticsearch.action.ingest.replica;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ReplicaOperationRequest extends TransportRequest implements Streamable {

    private long startTime;

    private String index;

    private int shardId;

    private int replicaId;

    private IngestReplicaShardRequest request;

    public ReplicaOperationRequest() {
    }

    public ReplicaOperationRequest(long startTime, String index, int shardId, int replicaId, IngestReplicaShardRequest request) {
        this.startTime = startTime;
        this.index = index;
        this.shardId = shardId;
        this.replicaId = replicaId;
        this.request = request;
    }

    public long startTime() {
        return startTime;
    }

    public String index() {
        return index;
    }

    public int shardId() {
        return shardId;
    }

    public int replicaId() {
        return replicaId;
    }

    public IngestReplicaShardRequest request() {
        return request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startTime = in.readLong();
        index = in.readString();
        shardId = in.readVInt();
        replicaId = in.readVInt();
        request = new IngestReplicaShardRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(startTime);
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeVInt(replicaId);
        request.writeTo(out);
    }
}

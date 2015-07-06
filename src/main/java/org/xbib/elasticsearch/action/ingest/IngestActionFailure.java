package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class IngestActionFailure implements Streamable {

    private long ingestId;

    private ShardId shardId;

    private String message;

    IngestActionFailure() {
    }

    public IngestActionFailure(long ingestId, ShardId shardId, String message) {
        this.ingestId = ingestId;
        this.shardId = shardId;
        this.message = message;
    }

    public static IngestActionFailure from(StreamInput in) throws IOException {
        IngestActionFailure itemFailure = new IngestActionFailure();
        itemFailure.readFrom(in);
        return itemFailure;
    }

    public long ingestId() {
        return ingestId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String message() {
        return message;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ingestId = in.readLong();
        if (in.readBoolean()) {
            shardId = ShardId.readShardId(in);
        }
        message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(ingestId);
        if (shardId != null) {
            out.writeBoolean(true);
            shardId.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeString(message);
    }

    public String toString() {
        return "[ingestId=" + ingestId + ",shardId=" + shardId + ",message=" + message + "]";
    }
}

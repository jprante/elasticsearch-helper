package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class DeleteActionFailure implements Streamable {

    private ShardId shardId;

    private String message;

    DeleteActionFailure() {
    }

    public DeleteActionFailure(ShardId shardId, String message) {
        this.shardId = shardId;
        this.message = message;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String message() {
        return message;
    }

    public static DeleteActionFailure from(StreamInput in) throws IOException {
        DeleteActionFailure itemFailure = new DeleteActionFailure();
        itemFailure.readFrom(in);
        return itemFailure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(message);
    }

    public String toString() {
        return "[shardId=" + shardId + ",message=" + message + "]";
    }
}

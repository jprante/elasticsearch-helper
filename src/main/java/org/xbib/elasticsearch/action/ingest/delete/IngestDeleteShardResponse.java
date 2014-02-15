
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import org.xbib.elasticsearch.action.ingest.IngestItemFailure;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class IngestDeleteShardResponse implements ActionResponse {

    private ShardId shardId;

    private int successSize;

    private List<IngestItemFailure> failure;

    public IngestDeleteShardResponse() {
        this.failure = newLinkedList();
    }

    public IngestDeleteShardResponse(ShardId shardId, int successSize, List<IngestItemFailure> failure) {
        this.shardId = shardId;
        this.successSize = successSize;
        this.failure = failure;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int successSize() {
        return successSize;
    }

    public List<IngestItemFailure> failure() {
        return failure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        successSize = in.readVInt();
        failure = newLinkedList();
        for (int i = 0; i < in.readVInt(); i++) {
            failure.add(new IngestItemFailure(in.readVInt(), in.readUTF()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(successSize);
        out.writeVInt(failure.size());
        for (IngestItemFailure f : failure) {
            out.writeVInt(f.pos());
            out.writeUTF(f.message());
        }
    }
}

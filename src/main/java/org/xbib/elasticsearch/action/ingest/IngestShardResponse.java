package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;

public class IngestShardResponse extends ActionResponse {

    private ShardId shardId;

    private int successSize;

    private List<IngestItemFailure> failure;

    public IngestShardResponse() {
        this.failure = Lists.newArrayList();
    }

    public IngestShardResponse(ShardId shardId, int successSize, List<IngestItemFailure> failure) {
        this.shardId = shardId;
        this.successSize = successSize;
        this.failure = failure;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public int getSuccessSize() {
        return successSize;
    }

    public List<IngestItemFailure> getFailure() {
        return failure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        successSize = in.readVInt();
        failure = Lists.newLinkedList();
        for (int i = 0; i < in.readVInt(); i++) {
            int pos = in.readVInt();
            String msg = in.readString();
            failure.add(new IngestItemFailure(pos, msg));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(successSize);
        out.writeVInt(failure.size());
        for (IngestItemFailure f : failure) {
            out.writeVInt(f.pos());
            out.writeString(f.message());
        }
    }
}

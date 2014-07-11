package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.action.index.replica.IndexReplicaShardResponse;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DeleteResponse extends ActionResponse {

    private String index;

    private String id;

    private String type;

    private long version;

    private boolean found;

    private Queue<IndexReplicaShardResponse> replicaResponses = new ConcurrentLinkedQueue<IndexReplicaShardResponse>();

    public DeleteResponse() {
    }

    public DeleteResponse setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return this.index;
    }

    public DeleteResponse setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return this.type;
    }

    public DeleteResponse setId(String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return this.id;
    }

    public DeleteResponse setVersion(long version) {
        this.version = version;
        return this;
    }

    public long getVersion() {
        return this.version;
    }

    public DeleteResponse setFound(boolean found) {
        this.found = found;
        return this;
    }

    public boolean isFound() {
        return found;
    }

    public void addReplicaResponses(List<IndexReplicaShardResponse> replicaShardResponseList) {
        this.replicaResponses.addAll(replicaShardResponseList);
    }

    public Queue<IndexReplicaShardResponse> getReplicaResponses() {
        return replicaResponses;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readSharedString();
        type = in.readSharedString();
        id = in.readString();
        version = in.readLong();
        found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeSharedString(index);
        out.writeSharedString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(found);
    }
}

package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.action.delete.replica.DeleteReplicaShardResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DeleteResponse extends ActionResponse {

    private String index;

    private String id;

    private String type;

    private long version;

    private boolean found;

    private int quorumShards;

    private DeleteActionFailure failure;

    @SuppressWarnings("unchecked")
    private List<DeleteReplicaShardResponse> replicaResponses = Collections.synchronizedList(new LinkedList());

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

    public DeleteResponse setQuorumShards(int quorumShards) {
        this.quorumShards = quorumShards;
        return this;
    }

    public int getQuorumShards() {
        return quorumShards;
    }

    public DeleteResponse addReplicaResponses(List<DeleteReplicaShardResponse> replicaShardResponseList) {
        this.replicaResponses.addAll(replicaShardResponseList);
        return this;
    }

    public List<DeleteReplicaShardResponse> getReplicaResponses() {
        return replicaResponses;
    }

    public DeleteResponse setFailure(DeleteActionFailure failure) {
        this.failure = failure;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readSharedString();
        type = in.readSharedString();
        id = in.readString();
        version = in.readLong();
        found = in.readBoolean();
        quorumShards = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DeleteReplicaShardResponse r = new DeleteReplicaShardResponse();
            r.readFrom(in);
            replicaResponses.add(r);
        }
        if (in.readBoolean()) {
            failure = new DeleteActionFailure();
            failure.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeSharedString(index);
        out.writeSharedString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(found);
        out.writeVInt(quorumShards);
        out.writeVInt(replicaResponses.size());
        for (DeleteReplicaShardResponse r : replicaResponses) {
            r.writeTo(out);
        }
        if (failure != null) {
            out.writeBoolean(true);
            failure.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}

package org.xbib.elasticsearch.action.index;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.action.index.replica.IndexReplicaShardResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class IndexResponse extends ActionResponse {

    private String index;

    private String id;

    private String type;

    private long version;

    private int quorumShards;

    private IndexActionFailure failure;

    @SuppressWarnings("unchecked")
    private List<IndexReplicaShardResponse> replicaResponses = Collections.synchronizedList(new LinkedList());

    public IndexResponse() {
    }

    public IndexResponse setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return this.index;
    }

    public IndexResponse setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return this.type;
    }

    public IndexResponse setId(String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return this.id;
    }

    public IndexResponse setVersion(long version) {
        this.version = version;
        return this;
    }

    public long getVersion() {
        return this.version;
    }

    public IndexResponse setQuorumShards(int quorumShards) {
        this.quorumShards = quorumShards;
        return this;
    }

    public int getQuorumShards() {
        return quorumShards;
    }

    public IndexResponse addReplicaResponses(List<IndexReplicaShardResponse> replicaShardResponseList) {
        this.replicaResponses.addAll(replicaShardResponseList);
        return this;
    }

    public List<IndexReplicaShardResponse> getReplicaResponses() {
        return replicaResponses;
    }

    public IndexResponse setFailure(IndexActionFailure failure) {
        this.failure = failure;
        return this;
    }

    public IndexActionFailure getFailure() {
        return failure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readSharedString();
        type = in.readSharedString();
        id = in.readString();
        version = in.readLong();
        replicaResponses = newLinkedList();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexReplicaShardResponse r = new IndexReplicaShardResponse();
            r.readFrom(in);
            replicaResponses.add(r);
        }
        if (in.readBoolean()) {
            failure = new IndexActionFailure();
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
        out.writeVInt(replicaResponses.size());
        for (IndexReplicaShardResponse r : replicaResponses) {
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

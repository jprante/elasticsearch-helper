package org.xbib.elasticsearch.action.index;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.action.index.replica.IndexReplicaShardResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class IndexResponse extends ActionResponse {

    private String index;

    private String id;

    private String type;

    private long version;

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

    public void addReplicaResponses(List<IndexReplicaShardResponse> replicaShardResponseList) {
        this.replicaResponses.addAll(replicaShardResponseList);
    }

    public List<IndexReplicaShardResponse> getReplicaResponses() {
        return replicaResponses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readSharedString();
        type = in.readSharedString();
        id = in.readString();
        version = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeSharedString(index);
        out.writeSharedString(type);
        out.writeString(id);
        out.writeLong(version);
    }
}

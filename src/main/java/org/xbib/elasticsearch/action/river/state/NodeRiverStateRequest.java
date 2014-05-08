package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeRiverStateRequest extends NodeOperationRequest {

    private String riverType;

    private String riverName;

    NodeRiverStateRequest() {
    }

    public NodeRiverStateRequest(String nodeId, RiverStateRequest request) {
        super(request, nodeId);
        this.riverName = request.getRiverName();
        this.riverType = request.getRiverType();
    }

    public NodeRiverStateRequest setRiverType(String riverType) {
        this.riverType = riverType;
        return this;
    }

    public String getRiverType() {
        return riverType;
    }

    public NodeRiverStateRequest setRiverName(String riverName) {
        this.riverName = riverName;
        return this;
    }

    public String getRiverName() {
        return riverName;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.riverName = in.readString();
        this.riverType = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(riverName);
        out.writeString(riverType);
    }
}

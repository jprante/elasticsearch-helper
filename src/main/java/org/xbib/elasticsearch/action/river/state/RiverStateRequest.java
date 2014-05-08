package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RiverStateRequest extends NodesOperationRequest<RiverStateRequest> {

    private String riverType;

    private String riverName;

    public RiverStateRequest setRiverType(String riverType) {
        this.riverType = riverType;
        return this;
    }

    public String getRiverType() {
        return riverType;
    }

    public RiverStateRequest setRiverName(String riverName) {
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

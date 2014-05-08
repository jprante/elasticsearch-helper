package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeRiverStateResponse extends NodeOperationResponse {

    private RiverState state;

    NodeRiverStateResponse() {
    }

    public NodeRiverStateResponse(DiscoveryNode node) {
        super(node);
    }

    public NodeRiverStateResponse setState(RiverState state) {
        this.state = state;
        return this;
    }

    public RiverState getState() {
        return state;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        boolean b = in.readBoolean();
        if (b) {
            state = new RiverState();
            state.readFrom(in);
        } else {
            state = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (state != null) {
            out.writeBoolean(true);
            state.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

}

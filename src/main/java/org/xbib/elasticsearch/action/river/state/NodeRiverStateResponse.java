package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class NodeRiverStateResponse extends NodeOperationResponse {

    private Set<RiverState> state;

    NodeRiverStateResponse() {
    }

    public NodeRiverStateResponse(DiscoveryNode node) {
        super(node);
        state = new TreeSet();
    }

    public NodeRiverStateResponse addState(RiverState state) {
        this.state.add(state);
        return this;
    }

    public Set<RiverState> getStates() {
        return state;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        state = new TreeSet();
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RiverState rs = new RiverState();
            rs.readFrom(in);
            state.add(rs);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(state.size());
        for (RiverState rs : state) {
            rs.writeTo(out);
        }
    }
}

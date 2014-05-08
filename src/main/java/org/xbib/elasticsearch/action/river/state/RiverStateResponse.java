package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

public class RiverStateResponse extends NodesOperationResponse implements ToXContent {

    private Set<RiverState> state;

    public RiverStateResponse() {
        state = new TreeSet();
    }

    public RiverStateResponse addState(RiverState state) {
        this.state.add(state);
        return this;
    }

    public Collection<RiverState> getStates() {
        return state;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("state", state);
        return builder;
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

package org.xbib.elasticsearch.action.retain;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class RetainRequest extends SingleShardRequest<RetainRequest> {

    private int delta;

    private int minToKeep;

    public RetainRequest(){
    }

    public RetainRequest(String index) {
        this.index(index);
    }

    public RetainRequest delta(int delta) {
        this.delta = delta;
        return this;
    }

    public RetainRequest minToKeep(int minToKeep) {
        this.minToKeep = minToKeep;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        delta = in.readVInt();
        minToKeep = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(delta);
        out.writeVInt(minToKeep);
    }

}

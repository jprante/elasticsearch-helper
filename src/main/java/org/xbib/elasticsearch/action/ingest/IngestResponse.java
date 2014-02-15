
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class IngestResponse implements ActionResponse {

    private List<IngestItemFailure> failure;

    private int successSize;

    private long tookInMillis;

    public IngestResponse() {
        this.failure = newLinkedList();
    }

    public IngestResponse(int successSize, List<IngestItemFailure> failure, long tookInMillis) {
        this.successSize = successSize;
        this.failure = failure;
        this.tookInMillis = tookInMillis;
    }

    public int successSize() {
        return successSize;
    }

    public int failureSize() {
        return failure.size();
    }

    public List<IngestItemFailure> failure() {
        return failure;
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue took() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue getTook() {
        return took();
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long tookInMillis() {
        return tookInMillis;
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis();
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        return !failure.isEmpty();
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (IngestItemFailure f : failure) {
            sb.append("\n[").append(f.pos()).append("], message [").append(f.message()).append("]");
        }
        return sb.toString();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        successSize = in.readVInt();
        failure = newLinkedList();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            failure.add(new IngestItemFailure(in.readVInt(), in.readUTF()));
        }
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(successSize);
        out.writeVInt(failure.size());
        for (IngestItemFailure f : failure) {
            out.writeVInt(f.pos());
            out.writeUTF(f.message());
        }
        out.writeVLong(tookInMillis);
    }
}

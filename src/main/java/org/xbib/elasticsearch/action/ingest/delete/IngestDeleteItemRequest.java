
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class IngestDeleteItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    IngestDeleteItemRequest() {
    }

    public IngestDeleteItemRequest(int id, ActionRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public static IngestDeleteItemRequest readBulkItem(StreamInput in) throws IOException {
        IngestDeleteItemRequest item = new IngestDeleteItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        request = new IngestDeleteRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        request.writeTo(out);
    }
}

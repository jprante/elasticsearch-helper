
package org.xbib.elasticsearch.action.ingest.create;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class IngestCreateItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    IngestCreateItemRequest() {
    }

    public IngestCreateItemRequest(int id, ActionRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public static IngestCreateItemRequest readBulkItem(StreamInput in) throws IOException {
        IngestCreateItemRequest item = new IngestCreateItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        request = new IngestCreateRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        request.writeTo(out);
    }
}

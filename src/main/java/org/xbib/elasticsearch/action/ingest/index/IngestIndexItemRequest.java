
package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class IngestIndexItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    IngestIndexItemRequest() {
    }

    public IngestIndexItemRequest(int id, ActionRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public static IngestIndexItemRequest readBulkItem(StreamInput in) throws IOException {
        IngestIndexItemRequest item = new IngestIndexItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        request = new IngestIndexRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        request.writeTo(out);
    }
}

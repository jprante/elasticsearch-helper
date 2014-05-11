package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class IngestItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    IngestItemRequest() {
    }

    public IngestItemRequest(int id, ActionRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public static IngestItemRequest readBulkItem(StreamInput in) throws IOException {
        IngestItemRequest item = new IngestItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        byte type = in.readByte();
        if (type == 0) {
            request = new IndexRequest();
        } else if (type == 1) {
            request = new DeleteRequest();
        }
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
        }
        request.writeTo(out);
    }
}

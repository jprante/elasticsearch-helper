package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

public class IngestRequestBuilder extends ActionRequestBuilder<IngestRequest, IngestResponse, IngestRequestBuilder> {

    public IngestRequestBuilder(ElasticsearchClient client, IngestAction action) {
        super(client, action, new IngestRequest());
    }

    public IngestRequestBuilder add(IndexRequest request) {
        this.request.add(request);
        return this;
    }

    public IngestRequestBuilder add(DeleteRequest request) {
        this.request.add(request);
        return this;
    }

    public IngestRequestBuilder add(byte[] data, int from, int length) throws Exception {
        this.request.add(data, from, length, null, null);
        return this;
    }

    public IngestRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        this.request.add(data, from, length, defaultIndex, defaultType);
        return this;
    }

    public IngestRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    public int numberOfActions() {
        return request.numberOfActions();
    }
}

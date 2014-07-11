package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.delete.DeleteRequestBuilder;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.action.index.IndexRequestBuilder;

public class IngestRequestBuilder extends ActionRequestBuilder<IngestRequest, IngestResponse, IngestRequestBuilder> {

    public IngestRequestBuilder(Client client) {
        this(client, new IngestRequest());
    }

    public IngestRequestBuilder(Client client, IngestRequest request) {
        super((InternalClient) client, request);
    }

    @Override
    public IngestRequest request() {
        return this.request;
    }

    @Override
    public ListenableActionFuture<IngestResponse> execute() {
        PlainListenableActionFuture<IngestResponse> future = new PlainListenableActionFuture<IngestResponse>(request.listenerThreaded(), client.threadPool());
        execute(future);
        return future;
    }

    @Override
    public void execute(ActionListener<IngestResponse> listener) {
        doExecute(listener);
    }

    public IngestRequestBuilder add(IndexRequest request) {
        this.request.add(request);
        return this;
    }

    public IngestRequestBuilder add(IndexRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    public IngestRequestBuilder add(DeleteRequest request) {
        this.request.add(request);
        return this;
    }

    public IngestRequestBuilder add(DeleteRequestBuilder request) {
        this.request.add(request.request());
        return this;
    }

    public IngestRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        this.request.add(data, from, length, contentUnsafe, null, null);
        return this;
    }

    public IngestRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        this.request.add(data, from, length, contentUnsafe, defaultIndex, defaultType);
        return this;
    }

    public IngestRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }

    protected void doExecute(ActionListener<IngestResponse> listener) {
        ((InternalClient) client).execute(IngestAction.INSTANCE, request, listener);
    }
}

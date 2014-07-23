package org.xbib.elasticsearch.action.support.replication.replica;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

public abstract class ReplicaShardOperationRequestBuilder<Request extends ReplicaShardOperationRequest<Request>, Response extends ActionResponse, RequestBuilder extends ReplicaShardOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder, Client> {

    protected ReplicaShardOperationRequestBuilder(Client client, Request request) {
        super(client, request);
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setOperationThreaded(boolean threadedOperation) {
        request.operationThreaded(threadedOperation);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndex(String index) {
        request.index(index);
        return (RequestBuilder) this;
    }

}

package org.xbib.elasticsearch.action.support.replication.leader;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

public abstract class LeaderShardOperationRequestBuilder<Request extends LeaderShardOperationRequest<Request>, Response extends ActionResponse, RequestBuilder extends LeaderShardOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder, Client> {

    protected LeaderShardOperationRequestBuilder(Client client, Request request) {
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

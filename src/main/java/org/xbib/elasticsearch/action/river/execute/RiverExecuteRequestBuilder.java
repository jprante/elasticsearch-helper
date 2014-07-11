package org.xbib.elasticsearch.action.river.execute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalGenericClient;

public class RiverExecuteRequestBuilder extends NodesOperationRequestBuilder<RiverExecuteRequest, RiverExecuteResponse, RiverExecuteRequestBuilder> {

    public RiverExecuteRequestBuilder(Client client) {
        super((InternalGenericClient) client, new RiverExecuteRequest());
    }

    public RiverExecuteRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public RiverExecuteRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<RiverExecuteResponse> listener) {
        ((Client) client).execute(RiverExecuteAction.INSTANCE, request, listener);
    }
}

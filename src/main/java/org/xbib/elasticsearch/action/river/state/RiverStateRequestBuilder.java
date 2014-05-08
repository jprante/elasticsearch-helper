package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.Client;

public class RiverStateRequestBuilder extends NodesOperationRequestBuilder<RiverStateRequest, RiverStateResponse, RiverStateRequestBuilder> {

    public RiverStateRequestBuilder(Client client) {
        super((org.elasticsearch.client.internal.InternalGenericClient) client, new RiverStateRequest());
    }

    public RiverStateRequestBuilder setRiverType(String riverType) {
        request.setRiverType(riverType);
        return this;
    }

    public RiverStateRequestBuilder setRiverName(String riverName) {
        request.setRiverName(riverName);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<RiverStateResponse> listener) {
        ((Client) client).execute(RiverStateAction.INSTANCE, request, listener);
    }
}

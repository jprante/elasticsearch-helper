package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class RiverStateAction extends Action<RiverStateRequest, RiverStateResponse, RiverStateRequestBuilder> {

    public static final RiverStateAction INSTANCE = new RiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.jdbc.state";

    private RiverStateAction() {
        super(NAME);
    }

    @Override
    public RiverStateRequestBuilder newRequestBuilder(Client client) {
        return new RiverStateRequestBuilder(client);
    }

    @Override
    public RiverStateResponse newResponse() {
        return new RiverStateResponse();
    }
}

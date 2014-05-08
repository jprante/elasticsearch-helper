package org.xbib.elasticsearch.action.river.execute;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class RiverExecuteAction extends Action<RiverExecuteRequest, RiverExecuteResponse, RiverExecuteRequestBuilder> {

    public static final RiverExecuteAction INSTANCE = new RiverExecuteAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.jdbc.execute";

    private RiverExecuteAction() {
        super(NAME);
    }

    @Override
    public RiverExecuteRequestBuilder newRequestBuilder(Client client) {
        return new RiverExecuteRequestBuilder(client);
    }

    @Override
    public RiverExecuteResponse newResponse() {
        return new RiverExecuteResponse();
    }
}

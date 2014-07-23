package org.xbib.elasticsearch.action.river.execute;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ClusterAdminClient;

public class RiverExecuteAction extends Action<RiverExecuteRequest, RiverExecuteResponse, RiverExecuteRequestBuilder, ClusterAdminClient> {

    public static final RiverExecuteAction INSTANCE = new RiverExecuteAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.jdbc.execute";

    private RiverExecuteAction() {
        super(NAME);
    }

    @Override
    public RiverExecuteRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new RiverExecuteRequestBuilder(client);
    }

    @Override
    public RiverExecuteResponse newResponse() {
        return new RiverExecuteResponse();
    }
}

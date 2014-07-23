package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ClusterAdminClient;

public class RiverStateAction extends Action<RiverStateRequest, RiverStateResponse, RiverStateRequestBuilder, ClusterAdminClient> {

    public static final RiverStateAction INSTANCE = new RiverStateAction();

    public static final String NAME = "org.xbib.elasticsearch.action.river.jdbc.state";

    private RiverStateAction() {
        super(NAME);
    }

    @Override
    public RiverStateRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new RiverStateRequestBuilder(client);
    }

    @Override
    public RiverStateResponse newResponse() {
        return new RiverStateResponse();
    }
}

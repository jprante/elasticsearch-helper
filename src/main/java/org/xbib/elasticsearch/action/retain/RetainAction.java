package org.xbib.elasticsearch.action.retain;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class RetainAction extends Action<RetainRequest, RetainResponse, RetainRequestBuilder> {

    public static final RetainAction INSTANCE = new RetainAction();

    public static final String NAME = "indices:admin/retain";

    public RetainAction() {
        super(NAME);
    }

    @Override
    public RetainResponse newResponse() {
        return new RetainResponse();
    }

    @Override
    public RetainRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RetainRequestBuilder(client, INSTANCE);
    }

}

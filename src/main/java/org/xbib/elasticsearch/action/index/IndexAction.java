package org.xbib.elasticsearch.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

/**
 */
public class IndexAction extends Action<IndexRequest, IndexResponse, IndexRequestBuilder> {

    public static final IndexAction INSTANCE = new IndexAction();
    public static final String NAME = "index";

    private IndexAction() {
        super(NAME);
    }

    @Override
    public IndexResponse newResponse() {
        return new IndexResponse();
    }

    @Override
    public IndexRequestBuilder newRequestBuilder(Client client) {
        return new IndexRequestBuilder(client);
    }
}

package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class DeleteAction extends Action<DeleteRequest, DeleteResponse, DeleteRequestBuilder> {

    public static final DeleteAction INSTANCE = new DeleteAction();

    public static final String NAME = "delete";

    private DeleteAction() {
        super(NAME);
    }

    @Override
    public DeleteResponse newResponse() {
        return new DeleteResponse();
    }

    @Override
    public DeleteRequestBuilder newRequestBuilder(Client client) {
        return new DeleteRequestBuilder(client);
    }
}

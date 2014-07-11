package org.xbib.elasticsearch.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.action.support.replication.leader.LeaderShardOperationRequestBuilder;

public class DeleteRequestBuilder extends LeaderShardOperationRequestBuilder<DeleteRequest, DeleteResponse, DeleteRequestBuilder> {

    public DeleteRequestBuilder(Client client) {
        super((InternalClient) client, new DeleteRequest());
    }

    public DeleteRequestBuilder(Client client, @Nullable String index) {
        super((InternalClient) client, new DeleteRequest(index));
    }

    public DeleteRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    public DeleteRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public DeleteRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    public DeleteRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public DeleteRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public DeleteRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    public DeleteRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DeleteResponse> listener) {
        ((Client) client).execute(DeleteAction.INSTANCE, request, listener);
    }
}

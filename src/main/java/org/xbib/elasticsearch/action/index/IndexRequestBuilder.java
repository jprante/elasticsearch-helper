package org.xbib.elasticsearch.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.action.support.replication.leader.LeaderShardOperationRequestBuilder;

import java.util.Map;

public class IndexRequestBuilder extends LeaderShardOperationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder> {

    public IndexRequestBuilder(Client client) {
        super(client, new IndexRequest());
    }

    public IndexRequestBuilder(Client client, @Nullable String index) {
        super(client, new IndexRequest(index));
    }

    public IndexRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    public IndexRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public IndexRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public IndexRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    public IndexRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.source(source, unsafe);
        return this;
    }

    public IndexRequestBuilder setSource(BytesReference source) {
        request.source(source, false);
        return this;
    }

    public IndexRequestBuilder setSource(Map<String, Object> source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    public IndexRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    public IndexRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    public IndexRequestBuilder setSource(byte[] source, int offset, int length, boolean unsafe) {
        request.source(source, offset, length, unsafe);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1) {
        request.source(field1, value1);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2) {
        request.source(field1, value1, field2, value2);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        request.source(field1, value1, field2, value2, field3, value3);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        request.source(field1, value1, field2, value2, field3, value3, field4, value4);
        return this;
    }

    public IndexRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setContentType(XContentType contentType) {
        request.contentType(contentType);
        return this;
    }

    public IndexRequestBuilder setOpType(IndexRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    public IndexRequestBuilder setOpType(String opType) {
        request.opType(opType);
        return this;
    }

    public IndexRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    public IndexRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public IndexRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    public IndexRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    public IndexRequestBuilder setTimestamp(String timestamp) {
        request.timestamp(timestamp);
        return this;
    }

    public IndexRequestBuilder setTTL(long ttl) {
        request.ttl(ttl);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<IndexResponse> listener) {
        client.execute(IndexAction.INSTANCE, request, listener);
    }
}

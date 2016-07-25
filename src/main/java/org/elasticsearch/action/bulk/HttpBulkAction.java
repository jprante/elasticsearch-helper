
package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.xbib.elasticsearch.helper.client.http.HttpAction;
import org.xbib.elasticsearch.helper.client.http.HttpInvocationContext;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpBulkAction extends HttpAction<BulkRequest, BulkResponse> {

    public HttpBulkAction(Settings settings) {
        super(settings, BulkAction.NAME);
    }

    @Override
    protected HttpRequest createHttpRequest(URL base, BulkRequest request) {
        StringBuilder bulkContent = new StringBuilder();
        for (ActionRequest actionRequest : request.requests()) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                bulkContent.append("{\"").append(indexRequest.opType().lowercase()).append("\":{");
                bulkContent.append("\"_index\":\"").append(indexRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append(indexRequest.type()).append("\"");
                if (indexRequest.id() != null) {
                    bulkContent.append(",\"_id\":\"").append(indexRequest.id()).append("\"");
                }
                if (indexRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(indexRequest.routing()).append("\""); // _routing
                }
                if (indexRequest.parent() != null) {
                    bulkContent.append(",\"_parent\":\"").append(indexRequest.parent()).append("\"");
                }
                if (indexRequest.timestamp() != null) {
                    bulkContent.append(",\"_timestamp\":\"").append(indexRequest.timestamp()).append("\"");
                }
                // avoid _ttl <= 0 at all cost!
                if (indexRequest.ttl() != null && indexRequest.ttl().seconds() > 0) {
                    bulkContent.append(",\"_ttl\":\"").append(indexRequest.ttl()).append("\"");
                }
                if (indexRequest.version() > 0) {
                    bulkContent.append(",\"_version\":\"").append(indexRequest.version()).append("\"");
                    if (indexRequest.versionType() != null) {
                        bulkContent.append(",\"_version_type\":\"").append(indexRequest.versionType().name()).append("\"");
                    }
                }
                bulkContent.append("}}\n");
                bulkContent.append(indexRequest.source().toUtf8());
                bulkContent.append("\n");
            } else if (actionRequest instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                bulkContent.append("{\"delete\":{");
                bulkContent.append("\"_index\":\"").append(deleteRequest.index()).append("\"");
                bulkContent.append(",\"_type\":\"").append(deleteRequest.type()).append("\"");
                bulkContent.append(",\"_id\":\"").append(deleteRequest.id()).append("\"");
                if (deleteRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(deleteRequest.routing()).append("\""); // _routing
                }
                bulkContent.append("}}\n");
            }
        }
        return newPostRequest(base, "/_bulk", bulkContent);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected BulkResponse createResponse(HttpInvocationContext<BulkRequest,BulkResponse> httpInvocationContext) {
        if (httpInvocationContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpInvocationContext.getHttpResponse();
        try {
            BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
            Map<String,Object> map = JsonXContent.jsonXContent.createParser(ref).map();
            long tookInMillis = map.containsKey("took") ? (Integer)map.get("took") : -1L;
            BulkItemResponse[] responses = parseItems((List<Map<String,?>>)map.get("items"));
            return new BulkResponse(responses, tookInMillis);
        } catch (IOException e) {
            //
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private BulkItemResponse[] parseItems(List<Map<String,?>> items) {
        List<BulkItemResponse> list = new ArrayList<>();
        int i = 0;
        for (Map<String,?> item: items) {
            if (item.containsKey(UPDATE_OP)) {
                item = (Map<String,?>)item.get(UPDATE_OP);
                String index = (String) item.get(INDEX);
                String type = (String) item.get(TYPE);
                String id = (String) item.get(ID);
                if (item.containsKey(ERROR)) {
                    ElasticsearchException e = new ElasticsearchException(item.get(ERROR).toString());
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, type, id, e);
                    list.add(new BulkItemResponse(i++, UPDATE_OP, failure));
                } else {
                    UpdateResponse updateResponse = new UpdateResponse(index, type, id,
                            item.containsKey(VERSION) ? (Integer) item.get(VERSION) : -1L,
                            false);
                    list.add(new BulkItemResponse(i++, UPDATE_OP, updateResponse));
                }
            } else if (item.containsKey(INDEX_OP)) {
                item = (Map<String,?>)item.get(INDEX_OP);
                String index = (String) item.get(INDEX);
                String type = (String) item.get(TYPE);
                String id = (String) item.get(ID);
                if (item.containsKey(ERROR)) {
                    ElasticsearchException e = new ElasticsearchException(item.get(ERROR).toString());
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, type, id, e);
                    list.add(new BulkItemResponse(i++, INDEX_OP, failure));
                } else {
                    int status = (Integer) item.get(STATUS);
                    IndexResponse indexResponse = new IndexResponse(index, type, id,
                            item.containsKey(VERSION) ? (Integer) item.get(VERSION) : -1L,
                            status == RestStatus.CREATED.getStatus());
                    list.add(new BulkItemResponse(i++, INDEX_OP, indexResponse));
                }
            } else if (item.containsKey(CREATE_OP)) {
                item = (Map<String,?>)item.get(CREATE_OP);
                String index = (String) item.get(INDEX);
                String type = (String) item.get(TYPE);
                String id = (String) item.get(ID);
                if (item.containsKey(ERROR)) {
                    ElasticsearchException e = new ElasticsearchException(item.get(ERROR).toString());
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, type, id, e);
                    list.add(new BulkItemResponse(i++, CREATE_OP, failure));
                } else {
                    int status = (Integer) item.get(STATUS);
                    IndexResponse indexResponse = new IndexResponse(index, type, id,
                            item.containsKey(VERSION) ? (Integer) item.get(VERSION) : -1L,
                            status == RestStatus.CREATED.getStatus());
                    list.add(new BulkItemResponse(i++, CREATE_OP, indexResponse));
                }
            } else if (item.containsKey(DELETE_OP)) {
                item = (Map<String,?>)item.get(DELETE_OP);
                String index = (String) item.get(INDEX);
                String type = (String) item.get(TYPE);
                String id = (String) item.get(ID);
                if (item.containsKey(ERROR)) {
                    ElasticsearchException e = new ElasticsearchException(item.get(ERROR).toString());
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, type, id, e);
                    list.add(new BulkItemResponse(i++, DELETE_OP, failure));
                } else {
                    int status = (Integer) item.get(STATUS);
                    DeleteResponse deleteResponse = new DeleteResponse(index, type, id,
                            item.containsKey(VERSION) ? (Integer) item.get(VERSION) : -1L,
                            status != RestStatus.NOT_FOUND.getStatus());
                    list.add(new BulkItemResponse(i++, DELETE_OP, deleteResponse));
                }
            }
        }
        return list.toArray(new BulkItemResponse[list.size()]);
    }

    private final static String INDEX = "_index";
    private final static String TYPE = "_type";
    private final static String ID = "_id";
    private final static String VERSION = "_version";
    private final static String INDEX_OP = "index";
    private final static String CREATE_OP = "create";
    private final static String DELETE_OP = "delete";
    private final static String UPDATE_OP = "update";
    private final static String ERROR = "error";
    private final static String STATUS = "status";

}

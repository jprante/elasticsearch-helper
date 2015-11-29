
package org.xbib.elasticsearch.helper.client.http.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.xbib.elasticsearch.helper.client.http.HttpAction;
import org.xbib.elasticsearch.helper.client.http.HttpContext;

import java.net.URL;

public class HttpBulkAction extends HttpAction<BulkRequest, BulkResponse> {

    public HttpBulkAction(Settings settings) {
        super(settings, BulkAction.NAME);
    }

    @Override
    protected HttpRequest createHttpRequest(URL base, BulkRequest request) {
        HttpRequest httpRequest =  newGetRequest(base);
        StringBuilder bulkContent = new StringBuilder();
        for (ActionRequest actionRequest : request.requests()) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                bulkContent.append("{\"").append(indexRequest.opType().lowercase()).append("\":{");
                bulkContent.append("\"index\":\"").append(indexRequest.index()).append("\"");
                bulkContent.append(",\"type\":\"").append(indexRequest.type()).append("\"");
                bulkContent.append(",\"id\":\"").append(indexRequest.id()).append("\"");
                if (indexRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(indexRequest.routing()).append("\""); // _routing
                }
                if (indexRequest.parent() != null) {
                    bulkContent.append(",\"_parent\":\"").append(indexRequest.parent()).append("\"");
                }
                if (indexRequest.timestamp() != null) {
                    bulkContent.append(",\"_timestamp\":\"").append(indexRequest.timestamp()).append("\"");
                }
                if (indexRequest.ttl() > 0) {
                    bulkContent.append(",\"_ttl\":\"").append(indexRequest.ttl()).append("\"");
                }
                if (indexRequest.version() > 0) {
                    bulkContent.append(",\"_version\":\"").append(indexRequest.version()).append("\"");
                }
                if (indexRequest.versionType() != null) {
                    bulkContent.append(",\"_version_type\":\"").append(indexRequest.versionType().name()).append("\"");
                }
                bulkContent.append("}}\n");
                bulkContent.append(indexRequest.source().toUtf8());
                bulkContent.append("\n");
            } else if (actionRequest instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
                bulkContent.append("{\"delete\":{");
                bulkContent.append("\"index\":\"").append(deleteRequest.index()).append("\"");
                bulkContent.append(",\"type\":\"").append(deleteRequest.type()).append("\"");
                bulkContent.append(",\"id\":\"").append(deleteRequest.id()).append("\"");
                if (deleteRequest.routing() != null) {
                    bulkContent.append(",\"_routing\":\"").append(deleteRequest.routing()).append("\""); // _routing
                }
                bulkContent.append("}}\n");
            }
        }
        httpRequest.setContent(ChannelBuffers.copiedBuffer(bulkContent, CharsetUtil.UTF_8));
        return httpRequest;
    }

    @Override
    protected void doExecute(final HttpContext httpContext) {
        httpContext.getChannel().write(httpContext.getHttpRequest());
    }

    @Override
    protected BulkResponse createResponse(HttpContext<BulkRequest,BulkResponse> httpContext) {
        HttpResponse httpResponse = httpContext.getHttpResponse();

        //public BulkResponse(BulkItemResponse[] responses, long tookInMillis)

        return null;
    }


}


package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.xbib.elasticsearch.helper.client.http.HttpAction;
import org.xbib.elasticsearch.helper.client.http.HttpInvocationContext;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class HttpRefreshIndexAction extends HttpAction<RefreshRequest, RefreshResponse> {

    public HttpRefreshIndexAction(Settings settings) {
        super(settings, RefreshAction.NAME);
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, RefreshRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newPostRequest(url, index + "/_refresh", null);
    }

    @Override
    protected RefreshResponse createResponse(HttpInvocationContext<RefreshRequest,RefreshResponse> httpInvocationContext) {
        if (httpInvocationContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpInvocationContext.getHttpResponse();
        try {
            BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
            Map<String,Object> map = JsonXContent.jsonXContent.createParser(ref).map();
            logger.info("{}", map);
            //  RefreshResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
            return new RefreshResponse();
        } catch (IOException e) {
            //
        }
        return null;
    }
}

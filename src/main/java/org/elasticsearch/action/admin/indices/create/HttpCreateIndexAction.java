
package org.elasticsearch.action.admin.indices.create;

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

public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    public HttpCreateIndexAction(Settings settings) {
        super(settings, CreateIndexAction.NAME);
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, CreateIndexRequest request) {
        return newPostRequest(url, "/" + request.index(), null);
    }

    @Override
    protected CreateIndexResponse createResponse(HttpInvocationContext<CreateIndexRequest,CreateIndexResponse> httpInvocationContext) {
        if (httpInvocationContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpInvocationContext.getHttpResponse();
        try {
            BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
            Map<String,Object> map = JsonXContent.jsonXContent.createParser(ref).map();
            boolean acknowledged = map.containsKey("acknowledged") ? (Boolean)map.get("acknowledged") : false;
            return new CreateIndexResponse(acknowledged);
        } catch (IOException e) {
            //
        }
        return null;
    }
}

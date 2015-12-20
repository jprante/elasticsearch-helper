package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.util.List;

public class HttpContext<Request extends ActionRequest, Response extends ActionResponse> {

    Channel channel;

    Request request;

    Response response;

    List<HttpChunk> chunks;

    ActionListener<Response> listener;

    HttpAction httpAction;

    HttpRequest httpRequest;

    HttpResponse httpResponse;

    long millis;

    public Channel getChannel() {
        return channel;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public List<HttpChunk> getChunks() {
        return chunks;
    }

}

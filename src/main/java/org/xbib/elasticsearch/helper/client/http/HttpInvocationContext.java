package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.util.List;

public class HttpInvocationContext<Request extends ActionRequest, Response extends ActionResponse> {

    private final HttpAction httpAction;

    private ActionListener<Response> listener;

    private final Request request;

    private final List<HttpChunk> chunks;

    private Channel channel;

    HttpRequest httpRequest;

    HttpResponse httpResponse;

    private long millis;

    HttpInvocationContext(HttpAction httpAction, ActionListener<Response> listener, List<HttpChunk> chunks, Request request) {
        this.httpAction = httpAction;
        this.listener = listener;
        this.chunks = chunks;
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }

    public HttpAction getHttpAction() {
        return httpAction;
    }

    public void setListener(ActionListener<Response> listener) {
        this.listener = listener;
    }

    public ActionListener<Response> getListener() {
        return listener;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

}

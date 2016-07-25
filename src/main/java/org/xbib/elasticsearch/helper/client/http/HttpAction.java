
package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

public abstract class HttpAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final String actionName;
    protected final ParseFieldMatcher parseFieldMatcher;

    protected HttpAction(Settings settings, String actionName) {
        super(settings);
        this.actionName = actionName;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
    }

    public final ActionFuture<Response> execute(HttpInvocationContext<Request,Response> httpInvocationContext, Request request) {
        PlainActionFuture<Response> future = newFuture();
        execute(httpInvocationContext, future);
        return future;
    }

    public final void execute(HttpInvocationContext<Request,Response> httpInvocationContext, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = httpInvocationContext.getRequest().validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        httpInvocationContext.setListener(listener);
        httpInvocationContext.setMillis(System.currentTimeMillis());
        try {
            doExecute(httpInvocationContext);
        } catch(Throwable t) {
            logger.error("exception during http action execution", t);
            listener.onFailure(t);
        }
    }

    protected HttpRequest newGetRequest(URL url, String path) {
        return newGetRequest(url, path, null);
    }

    protected HttpRequest newGetRequest(URL url, String path, CharSequence content) {
        return newRequest(HttpMethod.GET, url, path, content);
    }

    protected HttpRequest newPostRequest(URL url, String path, CharSequence content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, CharSequence content) {
        return newRequest(method, url, path, content != null ? ChannelBuffers.copiedBuffer(content, CharsetUtil.UTF_8) : null);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, BytesReference content) {
        return newRequest(method, url, path, content != null ? ChannelBuffers.copiedBuffer(content.toBytes()) : null);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, ChannelBuffer buffer) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, path);
        request.headers().add(HttpHeaders.Names.HOST, url.getHost());
        request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.headers().add(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        if (buffer != null) {
            request.setContent(buffer);
            int length = request.getContent().readableBytes();
            request.headers().add(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, length);
        }
        return request;
    }

    protected void doExecute(final HttpInvocationContext<Request,Response> httpInvocationContext) {
        httpInvocationContext.getChannel().write(httpInvocationContext.getHttpRequest());
    }

    protected abstract HttpRequest createHttpRequest(URL base, Request request) throws IOException;

    protected abstract Response createResponse(HttpInvocationContext<Request,Response> httpInvocationContext) throws IOException;

}

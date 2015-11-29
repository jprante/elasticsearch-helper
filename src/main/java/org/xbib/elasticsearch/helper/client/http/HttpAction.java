
package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

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

    public final ActionFuture<Response> execute(HttpContext<Request,Response> httpContext, Request request) {
        PlainActionFuture<Response> future = newFuture();
        execute(httpContext, future);
        return future;
    }

    public final void execute(HttpContext<Request,Response> httpContext, ActionListener<Response> listener) {
        ActionRequestValidationException validationException = httpContext.request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        httpContext.listener = listener;
        httpContext.millis = System.currentTimeMillis();
        try {
            doExecute(httpContext);
        } catch(Throwable t) {
            logger.error("exception during http action execution", t);
            listener.onFailure(t);
        }
    }

    protected HttpRequest newGetRequest(URL url) {
        return newRequest(url, HttpMethod.GET);
    }

    protected HttpRequest newPostRequest(URL url) {
        return newRequest(url, HttpMethod.POST);
    }

    protected HttpRequest newRequest(URL url, HttpMethod method) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, url.toExternalForm());
        request.headers().add(HttpHeaders.Names.HOST, url.getHost());
        request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.headers().add(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        return request;
    }

    protected abstract HttpRequest createHttpRequest(URL base, Request request);

    protected abstract void doExecute(HttpContext<Request,Response> httpContext);

    protected abstract Response createResponse(HttpContext<Request,Response> httpContext);

}

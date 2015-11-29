package org.xbib.elasticsearch.helper.client.http;

import com.google.common.collect.Maps;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.xbib.elasticsearch.helper.client.http.bulk.HttpBulkAction;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;

public class HttpElasticsearchClient extends AbstractClient {

    final Map<String, ActionEntry> actionMap = Maps.newHashMap();

    final Map<Channel, HttpContext> contextMap = Maps.newHashMap();

    static class ActionEntry<Request extends ActionRequest, Response extends ActionResponse> {
        public final GenericAction<Request, Response> action;
        public final HttpAction<Request, Response> httpAction;

        ActionEntry(GenericAction<Request, Response> action, HttpAction<Request, Response> httpAction) {
            this.action = action;
            this.httpAction = httpAction;
        }
    }

    ClientBootstrap bootstrap;

    URL url;

    class Builder {

        HttpElasticsearchClient client;

        String host;

        Integer port;

        Builder(Settings settings) {
            ThreadPool threadpool = new ThreadPool("http_client_pool");
            client = new HttpElasticsearchClient(settings, threadpool, null);
            client.registerAction(BulkAction.INSTANCE, HttpBulkAction.class);
        }

        public Builder url(URL base) {
            url = base;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public HttpElasticsearchClient build() {
            if (url == null && host != null && port != null) {
                try {
                    url = new URL(host + ":" + port);
                } catch (MalformedURLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (url == null) {
                throw new IllegalArgumentException("no base URL given");
            }
            client.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool()));
            bootstrap.setPipelineFactory(new HttpClientPipelineFactory());
            bootstrap.setOption("tcpNoDelay", true);
            return client;
        }
    }

    public Builder builder(Settings settings) {
        return new Builder(settings);
    }

    public HttpElasticsearchClient(Settings settings, ThreadPool threadPool, Headers headers) {
        super(settings, threadPool, headers);
    }

    @Override
    public void close() {
        bootstrap.releaseExternalResources();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        ActionEntry entry = actionMap.get(action.name());
        HttpAction<Request, Response> httpAction = entry.httpAction;
        if (httpAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        HttpContext<Request, Response> httpContext = new HttpContext();
        httpContext.request = request;
        httpContext.httpRequest = httpAction.createHttpRequest(this.url, request);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(url.getHost(), url.getPort()));
        if (!future.isSuccess()) {
            bootstrap.releaseExternalResources();
            logger.error("can't connect to {}", url);
        } else {
            Channel channel = future.getChannel();
            httpContext.channel = channel;
            contextMap.put(channel, httpContext);
            channel.getConfig().setConnectTimeoutMillis(5000);
            httpAction.execute(httpContext, listener);
        }
    }

    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void registerAction(GenericAction<Request, Response> action,
                                                                                                Class<? extends HttpAction<Request, Response>> httpAction) {
        try {
            HttpAction<Request, Response> instance = httpAction.getDeclaredConstructor(Settings.class).newInstance(settings);
            actionMap.put(action.name(), new ActionEntry<>(action, instance));
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e ) {
            logger.error(e.getMessage(), e);
        }
    }

    class HttpClientPipelineFactory implements ChannelPipelineFactory {

        HttpClientPipelineFactory() {
        }

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("codec", new HttpClientCodec());
            pipeline.addLast("inflater", new HttpContentDecompressor());
            pipeline.addLast("handler", new HttpResponseHandler());
            return pipeline;
        }
    }

    class HttpResponseHandler<Request extends ActionRequest, Response extends ActionResponse> extends SimpleChannelUpstreamHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = contextMap.remove(ctx.getChannel());
            if (httpContext == null) {
                throw new IllegalStateException("no context for channel?");
            }
            try {
                HttpAction<Request, Response> action = httpContext.httpAction;
                ActionListener<Response> listener = httpContext.listener;
                HttpResponse httpResponse = (HttpResponse) e.getMessage();
                httpContext.httpResponse = httpResponse;
                if (httpResponse.getContent().readable()) {
                    listener.onResponse(action.createResponse(httpContext));
                }
            } finally {
                // do we need to close connection?
                ctx.getChannel().close();
            }
        }

        @SuppressWarnings("unchecked")
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = contextMap.remove(ctx.getChannel());
            if (httpContext == null) {
                throw new IllegalStateException("no context for channel?");
            }
            try {
                httpContext.listener.onFailure(e.getCause());
            } finally {
                ctx.getChannel().close();
            }
        }
    }
}

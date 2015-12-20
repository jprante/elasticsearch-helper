package org.xbib.elasticsearch.helper.client.http;

import com.google.common.collect.Maps;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.HttpClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.refresh.HttpRefreshIndexAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.HttpUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.search.HttpSearchAction;
import org.elasticsearch.action.search.SearchAction;
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
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.elasticsearch.action.admin.indices.create.HttpCreateIndexAction;
import org.elasticsearch.action.bulk.HttpBulkAction;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;

public class HttpElasticsearchClient extends AbstractClient {

    final Map<String, ActionEntry> actionMap = Maps.newHashMap();

    final Map<Channel, HttpContext> contextMap;

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

    public static class Builder {

        HttpElasticsearchClient client;

        Settings settings;

        URL url;

        String host;

        Integer port;

        Builder(Settings settings) {
            this.settings = settings;
            try {
                this.url = settings.get("url") != null ? new URL(settings.get("url")) : null;
            } catch (MalformedURLException e) {
                // ignore
            }
            if (url == null) {
                this.host = settings.get("host", "127.0.0.1");
                this.port = settings.getAsInt("port", 9200);
            }
        }

        public Builder url(URL base) {
            this.url = base;
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
                    url = new URL("http://" + host + ":" + port);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException("malformed url: " + host + ":" + port);
                }
            }
            if (url == null) {
                throw new IllegalArgumentException("no base URL given");
            }
            ThreadPool threadpool = new ThreadPool("http_client_pool");
            client = new HttpElasticsearchClient(settings, threadpool, Headers.EMPTY, url);

            client.registerAction(BulkAction.INSTANCE, HttpBulkAction.class);
            client.registerAction(CreateIndexAction.INSTANCE, HttpCreateIndexAction.class);
            client.registerAction(RefreshAction.INSTANCE, HttpRefreshIndexAction.class);
            client.registerAction(ClusterUpdateSettingsAction.INSTANCE, HttpClusterUpdateSettingsAction.class);
            client.registerAction(UpdateSettingsAction.INSTANCE, HttpUpdateSettingsAction.class);
            client.registerAction(SearchAction.INSTANCE, HttpSearchAction.class);

            return client;
        }
    }

    public static Builder builder(Settings settings) {
        return new Builder(settings);
    }

    private HttpElasticsearchClient(Settings settings, ThreadPool threadPool, Headers headers, URL url) {
        super(settings, threadPool, headers);
        this.contextMap = Maps.newHashMap();
        this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new HttpClientPipelineFactory());
        bootstrap.setOption("tcpNoDelay", true);
        this.url = url;
    }

    @Override
    public void close() {
        bootstrap.releaseExternalResources();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        ActionEntry entry = actionMap.get(action.name());
        if (entry == null) {
            throw new IllegalStateException("no action entry for " + action.name());
        }
        HttpAction<Request, Response> httpAction = entry.httpAction;
        if (httpAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        HttpContext<Request, Response> httpContext = new HttpContext();
        httpContext.httpAction = httpAction;
        httpContext.listener = listener;
        httpContext.chunks = new LinkedList<>();
        httpContext.request = request;
        try {
            httpContext.httpRequest = httpAction.createHttpRequest(this.url, request);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(url.getHost(), url.getPort()));
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            bootstrap.releaseExternalResources();
            logger.error("can't connect to {}", url);
        } else {
            Channel channel = future.getChannel();
            httpContext.channel = channel;
            contextMap.put(channel, httpContext);
            channel.getConfig().setConnectTimeoutMillis(settings.getAsInt("http.client.timeout", 5000));
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

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpChunkAggregator(settings.getAsInt("http.client.maxchunksize", 10 * 1024 * 1024)));
            pipeline.addLast("inflater", new HttpContentDecompressor());
            pipeline.addLast("handler", new HttpResponseHandler());
            return pipeline;
        }
    }

    class HttpResponseHandler<Request extends ActionRequest, Response extends ActionResponse> extends SimpleChannelUpstreamHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = contextMap.get(ctx.getChannel());
            if (httpContext == null) {
                throw new IllegalStateException("no context for channel?");
            }
            try {
                if (e.getMessage() instanceof HttpResponse) {
                    HttpResponse httpResponse = (HttpResponse) e.getMessage();
                    HttpAction<Request, Response> action = httpContext.httpAction;
                    ActionListener<Response> listener = httpContext.listener;
                    httpContext.httpResponse = httpResponse;
                    if (httpResponse.getContent().readable() && listener != null && action != null) {
                        listener.onResponse(action.createResponse(httpContext));
                    }
                }
            } finally {
                ctx.getChannel().close();
                contextMap.remove(ctx.getChannel());
            }
        }

        @SuppressWarnings("unchecked")
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = contextMap.get(ctx.getChannel());
            try {
                if (httpContext != null && httpContext.listener != null) {
                    httpContext.listener.onFailure(e.getCause());
                } else {
                    logger.error(e.getCause().getMessage(), e.getCause());
                }
            } finally {
                ctx.getChannel().close();
                contextMap.remove(ctx.getChannel());
            }
        }
    }
}

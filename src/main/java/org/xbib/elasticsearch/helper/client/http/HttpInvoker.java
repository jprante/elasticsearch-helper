package org.xbib.elasticsearch.helper.client.http;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.HttpClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.HttpCreateIndexAction;
import org.elasticsearch.action.admin.indices.refresh.HttpRefreshIndexAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.HttpUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.HttpBulkAction;
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
import org.xbib.elasticsearch.helper.client.Future;
import org.xbib.elasticsearch.helper.client.RemoteInvoker;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;

public class HttpInvoker extends AbstractClient implements RemoteInvoker {

    private final Map<String, HttpElasticsearchClient.ActionEntry> actionMap = new HashMap();

    private final Map<Channel, HttpInvocationContext> contexts;

    private ClientBootstrap bootstrap;

    private URL url;

    static class ActionEntry<Request extends ActionRequest, Response extends ActionResponse> {
        public final GenericAction<Request, Response> action;
        public final HttpAction<Request, Response> httpAction;

        ActionEntry(GenericAction<Request, Response> action, HttpAction<Request, Response> httpAction) {
            this.action = action;
            this.httpAction = httpAction;
        }
    }

    public HttpInvoker(Settings settings, ThreadPool threadPool, Headers headers, URL url) {
        super(settings, threadPool, headers);
        this.contexts = new HashMap<>();
        this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new HttpInvoker.HttpClientPipelineFactory());
        bootstrap.setOption("tcpNoDelay", true);

        registerAction(BulkAction.INSTANCE, HttpBulkAction.class);
        registerAction(CreateIndexAction.INSTANCE, HttpCreateIndexAction.class);
        registerAction(RefreshAction.INSTANCE, HttpRefreshIndexAction.class);
        registerAction(ClusterUpdateSettingsAction.INSTANCE, HttpClusterUpdateSettingsAction.class);
        registerAction(UpdateSettingsAction.INSTANCE, HttpUpdateSettingsAction.class);
        registerAction(SearchAction.INSTANCE, HttpSearchAction.class);

        this.url = url;
    }

    @Override
    public <T> Future<T> invoke(Settings settings, Method method, Object... args) throws Exception {
        return null;
    }
    @Override
    public void close() {
        bootstrap.releaseExternalResources();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
            void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        HttpElasticsearchClient.ActionEntry entry = actionMap.get(action.name());
        if (entry == null) {
            throw new IllegalStateException("no action entry for " + action.name());
        }
        HttpAction<Request, Response> httpAction = entry.httpAction;
        if (httpAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        HttpInvocationContext<Request, Response> httpInvocationContext = new HttpInvocationContext(httpAction, listener, new LinkedList<>(), request);
        try {
            httpInvocationContext.httpRequest = httpAction.createHttpRequest(this.url, request);
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
            httpInvocationContext.setChannel(channel);
            contexts.put(channel, httpInvocationContext);
            channel.getConfig().setConnectTimeoutMillis(settings.getAsInt("http.client.timeout", 5000));
            httpAction.execute(httpInvocationContext, listener);
        }
    }

    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void registerAction(GenericAction<Request, Response> action, Class<? extends HttpAction<Request, Response>> httpAction) {
        try {
            HttpAction<Request, Response> instance = httpAction.getDeclaredConstructor(Settings.class).newInstance(settings);
            actionMap.put(action.name(), new HttpElasticsearchClient.ActionEntry<>(action, instance));
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e ) {
            logger.error(e.getMessage(), e);
        }
    }

    private class HttpClientPipelineFactory implements ChannelPipelineFactory {

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpChunkAggregator(settings.getAsInt("http.client.maxchunksize", 10 * 1024 * 1024)));
            pipeline.addLast("inflater", new HttpContentDecompressor());
            pipeline.addLast("handler", new HttpInvoker.HttpResponseHandler());
            return pipeline;
        }
    }

    private class HttpResponseHandler<Request extends ActionRequest, Response extends ActionResponse> extends SimpleChannelUpstreamHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpInvocationContext<Request, Response> httpInvocationContext = contexts.get(ctx.getChannel());
            if (httpInvocationContext == null) {
                throw new IllegalStateException("no context for channel?");
            }
            try {
                if (e.getMessage() instanceof HttpResponse) {
                    HttpResponse httpResponse = (HttpResponse) e.getMessage();
                    HttpAction<Request, Response> action = httpInvocationContext.getHttpAction();
                    ActionListener<Response> listener = httpInvocationContext.getListener();
                    httpInvocationContext.httpResponse = httpResponse;
                    if (httpResponse.getContent().readable() && listener != null && action != null) {
                        listener.onResponse(action.createResponse(httpInvocationContext));
                    }
                }
            } finally {
                ctx.getChannel().close();
                contexts.remove(ctx.getChannel());
            }
        }

        @SuppressWarnings("unchecked")
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            HttpInvocationContext<Request, Response> httpInvocationContext = contexts.get(ctx.getChannel());
            try {
                if (httpInvocationContext != null && httpInvocationContext.getListener() != null) {
                    httpInvocationContext.getListener().onFailure(e.getCause());
                } else {
                    logger.error(e.getCause().getMessage(), e.getCause());
                }
            } finally {
                ctx.getChannel().close();
                contexts.remove(ctx.getChannel());
            }
        }
    }
}

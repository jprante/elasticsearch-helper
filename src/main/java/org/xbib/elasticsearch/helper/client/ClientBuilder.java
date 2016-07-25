/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.xbib.elasticsearch.helper.client.http.HttpInvoker;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URL;

public final class ClientBuilder implements ClientParameters {

    private final Settings.Builder settingsBuilder;

    private IngestMetric metric;

    public ClientBuilder() {
        settingsBuilder = Settings.builder();
    }

    public static ClientBuilder builder() {
        return new ClientBuilder();
    }

    public ClientBuilder put(String key, String value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Integer value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Long value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Double value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, ByteSizeValue value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, TimeValue value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(Settings settings) {
        settingsBuilder.put(settings);
        return this;
    }

    public ClientBuilder setMetric(IngestMetric metric) {
        this.metric = metric;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T build(URL url, Class<T> interfaceClass) {
        Settings settings = settingsBuilder.build();
        ThreadPool threadpool = new ThreadPool("http_client_pool");
        Headers headers = Headers.EMPTY;
        final RemoteInvoker remoteInvoker = new HttpInvoker(settings, threadpool, headers, url);
        final InvocationHandler handler =
                new ClientInvocationHandler(remoteInvoker, interfaceClass, settings);
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class[]{ interfaceClass }, handler);
    }

    public BulkNodeClient toBulkNodeClient(Client client) {
        Settings settings = settingsBuilder.build();
        return new BulkNodeClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(client, metric);
    }

    public BulkTransportClient toBulkTransportClient() {
        Settings settings = settingsBuilder.build();
        return new BulkTransportClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric);
    }

    public IngestTransportClient toIngestTransportClient() {
        Settings settings = settingsBuilder.build();
        return new IngestTransportClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric);
    }

    public HttpBulkNodeClient toHttpBulkNodeClient() {
        Settings settings = settingsBuilder.build();
        return new HttpBulkNodeClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric);
    }

    public MockTransportClient toMockTransportClient() {
        Settings settings = settingsBuilder.build();
        return new MockTransportClient()
                .maxActionsPerRequest(settings.getAsInt(MAX_ACTIONS_PER_REQUEST, DEFAULT_MAX_ACTIONS_PER_REQUEST))
                .maxConcurrentRequests(settings.getAsInt(MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS))
                .maxVolumePerRequest(settings.getAsBytesSize(MAX_VOLUME_PER_REQUEST, DEFAULT_MAX_VOLUME_PER_REQUEST))
                .flushIngestInterval(settings.getAsTime(FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL))
                .init(settings, metric);
    }

}

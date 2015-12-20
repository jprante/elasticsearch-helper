package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class ClientBuilder {

    public final static String MAX_ACTIONS_PER_REQUEST = "max_actions_per_request";

    public final static String MAX_CONCURRENT_REQUESTS = "max_concurrent_requests";

    public final static String MAX_VOLUME_PER_REQUEST = "max_volume_per_request";

    public final static String FLUSH_INTERVAL = "flush_interval";

    public final static int DEFAULT_MAX_ACTIONS_PER_REQUEST = 1000;

    public final static int DEFAULT_MAX_CONCURRENT_REQUESTS = Runtime.getRuntime().availableProcessors() * 4;

    public final static ByteSizeValue DEFAULT_MAX_VOLUME_PER_REQUEST = new ByteSizeValue(10, ByteSizeUnit.MB);

    public final static TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(30);

    private Settings.Builder settingsBuilder;

    private IngestMetric metric;

    private ClientBuilder() {
    }

    public static ClientBuilder builder() {
        ClientBuilder clientBuilder = new ClientBuilder();
        clientBuilder.settingsBuilder = Settings.builder();
        return clientBuilder;
    }

    public ClientBuilder put(String key, String value) {
        settingsBuilder.put(key, value);
        return this;
    }

    public ClientBuilder put(String key, Integer value) {
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

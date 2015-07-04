package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface Ingest {

    int DEFAULT_MAX_ACTIONS_PER_REQUEST = 1000;

    int DEFAULT_MAX_CONCURRENT_REQUESTS = Runtime.getRuntime().availableProcessors() * 4;

    ByteSizeValue DEFAULT_MAX_VOLUME_PER_REQUEST = new ByteSizeValue(10, ByteSizeUnit.MB);

    TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(30);

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Ingest index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this
     */
    Ingest delete(String index, String type, String id);

    Ingest newClient(Client client) throws IOException;

    Ingest newClient(Settings settings) throws IOException;

    Ingest newClient(Map<String,String> settings) throws IOException;

    AbstractClient client();

    /**
     * Set the maximum number of actions per request
     *
     * @param maxActionsPerRequest maximum number of actions per request
     * @return this ingest
     */
    Ingest maxActionsPerRequest(int maxActionsPerRequest);

    /**
     * Set the maximum concurent requests
     *
     * @param maxConcurentRequests maximum number of concurrent ingest requests
     * @return this Ingest
     */
    Ingest maxConcurrentRequests(int maxConcurentRequests);

    /**
     * Set the maximum volume for request before flush
     *
     * @param maxVolume maximum volume
     * @return this ingest
     */
    Ingest maxVolumePerRequest(ByteSizeValue maxVolume);

    /**
     * Set the flush interval for automatic flushing outstanding ingest requests
     *
     * @param flushInterval the flush interval, default is 30 seconds
     * @return this ingest
     */
    Ingest flushIngestInterval(TimeValue flushInterval);

    Settings.Builder getSettingsBuilder();

    /**
     * Create settings
     *
     * @param in the input stream with settings
     */
    void setting(InputStream in) throws IOException;

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, String value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Boolean value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Integer value);

    void mapping(String type, InputStream in) throws IOException;

    void mapping(String type, String mapping) throws IOException;

    Map<String, String> getMappings();

    Ingest putMapping(String index);

    /**
     * Create a new index
     *
     * @return this ingest
     */
    Ingest newIndex(String index);

    Ingest newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException;

    Ingest newIndex(String index, Settings settings, Map<String,String> mappings);

    /**
     * Create new mapping
     * @param index index
     * @param type index type
     * @param mapping mapping
     * @return this ingest
     */
    Ingest newMapping(String index, String type, Map<String,Object> mapping);

    /**
     * Delete index
     *
     * @return this ingest
     */
    Ingest deleteIndex(String index);

    /**
     * Start bulk mode
     *
     * @return this ingest
     */
    Ingest startBulk(String index, long startRefreshInterval, long stopRefreshInterval) throws IOException;

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this Ingest
     */
    Ingest stopBulk(String index) throws IOException;

    /**
     * Bulked index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * @param indexRequest the index request to add
     * @return this ingest
     */
    Ingest bulkIndex(IndexRequest indexRequest);

    /**
     * Bulked delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * @param deleteRequest the delete request to add
     * @return this ingest
     */
    Ingest bulkDelete(DeleteRequest deleteRequest);

    /**
     * Flush ingest, move all pending documents to the cluster.
     *
     * @return this
     */
    Ingest flushIngest();

    /**
     * Wait for all outstanding responses
     *
     * @param maxWait maximum wait time
     * @return this ingest
     * @throws InterruptedException
     */
    Ingest waitForResponses(TimeValue maxWait) throws InterruptedException;

    /**
     * Refresh the index.
     *
     * @return this ingest
     */
    Ingest refreshIndex(String index);

    Ingest flushIndex(String index);

    /**
     * Add replica level.
     *
     * @param level the replica level
     * @return number of shards after updating replica level
     */
    int updateReplicaLevel(String index, int level) throws IOException;

    /**
     * Wait for cluster being healthy.
     *
     * @throws IOException
     */
    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException;

    /**
     * Wait for index recovery (after replica change)
     *
     * @return number of shards found
     */
    int waitForRecovery(String index) throws IOException;

    Ingest setMetric(Metric metric);

    Metric getMetric();

    boolean hasThrowable();

    /**
     * Return last throwable if exists.
     *
     * @return last throwable
     */
    Throwable getThrowable();

    /**
     * Shutdown the ingesting
     */
    void shutdown();
}

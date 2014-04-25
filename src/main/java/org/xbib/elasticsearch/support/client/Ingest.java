
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface Ingest extends Feeder {

    Ingest newClient(Client client);

    Ingest newClient(URI uri);

    /**
     * Set index name
     * @param index the index name
     * @return this
     */
    Ingest setIndex(String index);

    /**
     * Set type name
     * @param type the type
     * @return this
     */
    Ingest setType(String type);

    /**
     * Set the maximum number of actions per bulk request
     *
     * @param maxActions maximum number of bulk actions
     * @return this ingest
     */
    Ingest maxActionsPerBulkRequest(int maxActions);

    /**
     * Set the maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests maximum number of concurrent bulk requests
     * @return this Ingest
     */
    Ingest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Set the maximum volume for bulk request before flush
     * @param maxVolume maximum volume
     * @return this ingest
     */
    Ingest maxVolumePerBulkRequest(ByteSizeValue maxVolume);

    /**
     * Set request timeout. Default is 60s.
     * @param timeout timeout
     * @return this ingest
     */
    Ingest maxRequestWait(TimeValue timeout);

    /**
     * The number of shards for index creation
     * @param shards the number of shards
     * @return this
     */
    Ingest shards(int shards);

    /**
     * The number of replica for index creation
     * @param replica the number of replica
     * @return this
     */
    Ingest replica(int replica);

    Settings settings();

    /**
     * Clear settings
     * @return this
     */
    Ingest resetSettings();

    /**
     * Create a key/value in the settings
     * @param key the key
     * @param value the value
     * @return this
     */
    Ingest setting(String key, String value);

    /**
     * Create a key/value in the settings
     * @param key the key
     * @param value the value
     * @return this
     */
    Ingest setting(String key, Boolean value);

    /**
     * Create a key/value in the settings
     * @param key the key
     * @param value the value
     * @return this
     */
    Ingest setting(String key, Integer value);

    /**
     * Create a key/value in the settings
     * @param in the input stream with settings
     * @return this
     */
    Ingest setting(InputStream in) throws IOException;

    Ingest mapping(String type, InputStream in) throws IOException;

    Ingest mapping(String type, String mapping);

    /**
     * Start bulk mode
     *
     * @return this ingest
     */
    Ingest startBulk() throws IOException;

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this Ingest
     */
    Ingest stopBulk() throws IOException;

    /**
     * Create a new index
     *
     * @return this ingest
     */
    Ingest newIndex();

    /**
     * Delete index
     *
     * @return this ingest
     */
    Ingest deleteIndex();

    /**
     * Flush ingest, move all pending documents to the bulk indexer
     * @return this
     */
    Ingest flush();

    /**
     * Refresh the index.
     *
     * @return this ingest
     */
    Ingest refresh();

    /**
     * Add replica level.
     *
     * @param level the replica level
     * @return number of shards after updating replica level
     */
    int updateReplicaLevel(int level) throws IOException;

    /**
     * Wait for cluster being healthy.
     * @throws IOException
     */
    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException;

    /**
     * Wait for index recovery (after replica change)
     * @return number of shards found
     */
    int waitForRecovery() throws IOException;

    State getState();

    boolean hasThrowable();

    /**
     * Return last throwable if exists.
     * @return last throwable
     */
    Throwable getThrowable();

    /**
     * Shutdown the ingesting
     */
    void shutdown();
}

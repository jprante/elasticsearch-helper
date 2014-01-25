
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface Ingest extends ClientBuilder, Feeder {

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

    /**
     * Enable date format detection in strings in the settings
     * @param b true if  date format detection should be enabled
     * @return this
     */
    Ingest dateDetection(boolean b);

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
    void waitForCluster() throws IOException;

    /**
     * Wait for index recovery (after replica change)
     * @return number of shards found
     */
    int waitForRecovery();

    /**
     * Get total bulk request count
     * @return the total bulk request
     */
    long getTotalBulkRequests();

    /**
     * Get total bulk request time spent
     * @return the total bulk request time
     */
    long getTotalBulkRequestTime();

    /**
     * Get total documents that have been ingested
     * @return the total number of documents
     */
    long getTotalDocuments();

    /**
     * Get the total ingested data size in bytes so far.
     *
     * @return the total size in bytes
     */
    long getTotalSizeInBytes();

    boolean hasErrors();

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

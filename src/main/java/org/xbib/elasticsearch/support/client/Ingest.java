
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for providing convenient ingest methods.
 */
public interface Ingest extends Feeder {

    Ingest setIndex(String index);

    Ingest setType(String type);

    Ingest dateDetection(boolean b);

    /**
     * Wait for healthy cluster
     *
     * @return this ingest
     * @throws IOException
     */
    Ingest waitForCluster() throws IOException;

    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timevalue) throws IOException;

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

    Ingest shards(int shards);

    Ingest replica(int replica);

    Ingest resetSettings();

    Ingest setting(String key, String value);

    Ingest setting(String key, Boolean value);

    Ingest setting(String key, Integer value);

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

    int waitForRecovery();

    long getTotalBulkRequests();

    long getTotalBulkRequestTime();

    long getTotalDocuments();

    /**
     * Get the total ingested data size in bytes so far.
     *
     * @return the total size in bytes
     */
    long getTotalSizeInBytes();

    boolean hasErrors();

    Throwable getThrowable();
}

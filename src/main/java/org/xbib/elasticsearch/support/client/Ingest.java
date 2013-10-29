
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Interface for providing convenient ingest methods.
 */
public interface Ingest extends DocumentIngest {

    /**
     * Wait for healthy cluster
     *
     * @return this TransportClientIndexer
     * @throws java.io.IOException
     */
    Ingest waitForCluster() throws IOException;

    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timevalue) throws IOException;

    int waitForRecovery();

    /**
     * Set maximum number of bulk actions
     *
     * @param bulkActions maximum number of bulk actions
     * @return this TransportClientIndexer
     */
    Ingest maxBulkActions(int bulkActions);

    /**
     * Set maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests maximum number of concurrent bulk requests
     * @return this Ingest
     */
    Ingest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Start bulk mode
     *
     * @return this Ingest
     */
    Ingest startBulk() throws IOException;

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this Ingest
     */
    Ingest stopBulk() throws IOException;

    /**
     * Settings for the index
     * @param key the settings key
     * @param value the settings value
     * @return this Ingest
     */
    Ingest setting(String key, String value);

    /**
     * Settings for the index
     * @param key the settings key
     * @param value the settings value
     * @return this Ingest
     */
    Ingest setting(String key, Integer value);

    /**
     * Settings for the index
     * @param key the settings key
     * @param value the settings value
     * @return this Ingest
     */
    Ingest setting(String key, Boolean value);

    /**
     * Settings for the index
     * @param in the input stream to read the settings from
     * @return this Ingest
     */
    Ingest setting(InputStream in) throws IOException;

    /**
     * Create a new index
     *
     * @return this Ingest
     */
    Ingest newIndex();

    Ingest mapping(InputStream in) throws IOException;

    Ingest newMapping(String type);

    /**
     * Delete index
     *
     * @return this Ingest
     */
    Ingest deleteIndex();

    Ingest deleteMapping(String type);

    /**
     * Refresh the index.
     *
     * @return
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
     * Get the ingested data volume so far.
     *
     * @return the volume in bytes
     */
    long getVolumeInBytes();

    Ingest shards(int shards);

    Ingest replica(int replica);



}

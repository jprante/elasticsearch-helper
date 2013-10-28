
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

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
     * @param maxConcurentBulkRequests maximum umber of concurrent bulk requests
     * @return this TransportClientIndexer
     */
    Ingest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Start bulk mode. Disables refresh.
     *
     * @return this TransportClientIndexer
     */
    Ingest startBulkMode();

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this TransportClientIndexer
     */
    Ingest stopBulkMode();

    Ingest shards(int shards);

    Ingest replica(int replica);

    Ingest setting(String key, String value);

    Ingest setting(String key, Integer value);

    Ingest setting(String key, Boolean value);

    /**
     * Create a new index
     *
     * @return this TransportClientIndexer
     */
    Ingest newIndex();

    Ingest newIndex(boolean ignoreExceptions);

    /**
     * Delete index
     *
     * @return this TransportClientIndexer
     */
    Ingest deleteIndex();

    Ingest newType();

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

}

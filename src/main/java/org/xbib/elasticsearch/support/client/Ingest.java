
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Interface for providing convenient ingest methods.
 */
public interface Ingest extends DocumentIngest {

    /**
     * Wait for healthy cluster
     *
     * @return this ingest
     * @throws IOException
     */
    Ingest waitForCluster() throws IOException;

    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timevalue) throws IOException;

    int waitForRecovery();

    /**
     * Set the maximum number of bulk actions
     *
     * @param bulkActions maximum number of bulk actions
     * @return this ingest
     */
    Ingest maxBulkActions(int bulkActions);

    /**
     * Set the  maximum concurent bulk requests
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
    Ingest maxVolume(ByteSizeValue maxVolume);

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

    /**
     * Get the ingested data volume so far.
     *
     * @return the volume in bytes
     */
    long getVolumeInBytes();

    Ingest shards(int shards);

    Ingest replica(int replica);

}

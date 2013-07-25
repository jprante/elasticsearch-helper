/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.support;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * TransportClientIngest is an interface for providing convenient ingest methods.
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public interface ClientIngester {

    /**
     * Wait for healthy cluster
     *
     * @return this TransportClientIndexer
     * @throws java.io.IOException
     */
    ClientIngester waitForCluster() throws IOException;

    ClientIngester waitForCluster(ClusterHealthStatus status, TimeValue timevalue) throws IOException;

    /**
     * Set maximum number of bulk actions
     *
     * @param bulkActions
     * @return this TransportClientIndexer
     */
    ClientIngester maxBulkActions(int bulkActions);

    /**
     * Set maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests
     * @return this TransportClientIndexer
     */
    ClientIngester maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Start bulk mode. Disables refresh.
     *
     * @return this TransportClientIndexer
     */
    ClientIngester startBulkMode();

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this TransportClientIndexer
     */
    ClientIngester stopBulkMode();

    ClientIngester shards(int shards);

    ClientIngester replica(int replica);

    ClientIngester setting(String key, String value);

    ClientIngester setting(String key, Integer value);

    ClientIngester setting(String key, Boolean value);

    /**
     * Create a new index
     *
     * @return this TransportClientIndexer
     */
    ClientIngester newIndex();

    ClientIngester newIndex(boolean ignoreExceptions);

    /**
     * Delete index
     *
     * @return this TransportClientIndexer
     */
    ClientIngester deleteIndex();

    ClientIngester newType();

    ClientIngester refresh();

    /**
     * Add replica level.
     *
     * @param level
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

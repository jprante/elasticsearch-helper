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
package org.elasticsearch.client.support;

import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

/**
 * TransportClientIngest is an interface for providing convenient ingest methods.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface TransportClientIngest {

    /**
     * Set settings
     *
     * @param settings the settings
     * @return this TransportClientIndexer
     */
    TransportClientIngest settings(Settings settings);

    /**
     * Create a new client
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest newClient();

    /**
     * Create a new client
     *
     * @param uri the URI to connect to
     * @return this TransportClientIndexer
     */
    TransportClientIngest newClient(URI uri);

    /**
     * Wait for healthy cluster
     *
     * @return this TransportClientIndexer
     * @throws java.io.IOException
     */
    TransportClientIngest waitForHealthyCluster() throws IOException;

    /**
     * Set type
     *
     * @param type the type
     * @return this TransportClientIndexer
     */
    TransportClientIngest type(String type);

    /**
     * Returns the type
     *
     * @return the type
     */
    String type();

    /**
     * Enable or disable automatic date detection
     *
     * @param dateDetection
     * @return this TransportClientIndexer
     */
    TransportClientIngest dateDetection(boolean dateDetection);

    /**
     * Set maximum number of bulk actions
     *
     * @param bulkActions
     * @return this TransportClientIndexer
     */
    TransportClientIngest maxBulkActions(int bulkActions);

    /**
     * Set maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests
     * @return this TransportClientIndexer
     */
    TransportClientIngest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Create document
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return this TransportClientIndexer
     */
    TransportClientIngest create(String index, String type, String id, String source);

    /**
     * Index document
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return this TransportClientIndexer
     */
    TransportClientIngest index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index
     * @param type
     * @param id
     * @return this TransportClientIndexer
     */
    TransportClientIngest delete(String index, String type, String id);

    /**
     * Ensure that all bulk
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest flush();

    /**
     * Start bulk mode. Disables refresh.
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest startBulkMode();

    /**
     * Stops bulk mode. Enables refresh.
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest stopBulkMode();

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

    /**
     * Create a new index
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest newIndex();

    /**
     * Delete index
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest deleteIndex();

    /**
     * Shutdown this Elasticsearch Indexer
     */
    void shutdown();

}

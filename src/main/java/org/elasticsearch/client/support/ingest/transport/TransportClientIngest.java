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
package org.elasticsearch.client.support.ingest.transport;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.ingest.ClientIngest;

import java.io.IOException;
import java.net.URI;

/**
 * TransportClientIngest is an interface for providing convenient ingest methods.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface TransportClientIngest extends ClientIngest {

    /**
     * Set the default index
     *
     * @param index the index
     * @return this TransportClientIndexer
     */
    TransportClientIngest setIndex(String index);

    /**
     * Set the default type
     *
     * @param type the type
     * @return this TransportClientIndexer
     */
    TransportClientIngest setType(String type);

    /**
     * Create a new transport client
     *
     * @return this TransportClientIndexer
     */
    TransportClientIngest newClient();

    /**
     * Create a new transport client
     *
     * @param uri the URI to connect to
     * @return this TransportClientIndexer
     */
    TransportClientIngest newClient(URI uri);

    Client client();

    /**
     * Get connection status
     * @return true is connected
     */
   boolean isConnected();

    /**
     * Wait for healthy cluster
     *
     * @return this TransportClientIndexer
     * @throws java.io.IOException
     */
    TransportClientIngest waitForHealthyCluster() throws IOException;

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

    TransportClientIngest mapping(String mapping);

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

    TransportClientIngest newType();

    TransportClientIngest refresh();

    /**
     *
     * Shutdown this client
     */
    void shutdown();

}

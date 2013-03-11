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
package org.elasticsearch.client.support.search.transport;

import org.elasticsearch.action.search.support.BasicRequest;

import java.io.IOException;
import java.net.URI;

/**
 * Transport client search helper API
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public interface TransportClientSearch {

    /**
     * Set index
     *
     * @param index
     * @return this TransportClientSearch
     */
    TransportClientSearch setIndex(String index);

    /**
     * Get index
     *
     * @return the index
     */
    String getIndex();

    /**
     * Set index
     *
     * @param type
     * @return this TransportClientSearch
     */
    TransportClientSearch setType(String type);

    /**
     * Get type
     *
     * @return the type
     */
    String getType();

    /**
     * Create a new client
     */
    TransportClientSearch newClient();

    /**
     * Create a new client
     */
    TransportClientSearch newClient(URI uri);

    boolean isConnected();

    /**
     * Create new search request
     */
    BasicRequest newSearchRequest();

    /**
     * Create new get request
     */
    BasicRequest newGetRequest();

    /**
     * Shutdown, free all resources
     */
    void shutdown();

}

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
package org.xbib.elasticsearch.support.search.transport;

import org.xbib.elasticsearch.action.search.support.BasicRequest;
import org.elasticsearch.client.Client;

import java.net.URI;

/**
 * Transport client search helper API
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public interface SearchClient {

    Client client();

    /**
     * Set index
     *
     * @param index
     * @return this TransportClientHelper
     */
    SearchClient setIndex(String index);

    /**
     * Get index
     *
     * @return the index
     */
    String getIndex();

    String getType();

    /**
     * Create a new client
     */
    SearchClient newClient();

    /**
     * Create a new client
     */
    SearchClient newClient(URI uri);

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

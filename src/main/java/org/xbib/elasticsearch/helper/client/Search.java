/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.action.search.helper.BasicGetRequest;
import org.xbib.elasticsearch.action.search.helper.BasicSearchRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Search support
 */
public interface Search {

    Search init(Settings settings) throws IOException;

    Search init(Map<String, String> settings) throws IOException;

    /**
     * Return the Elasticsearch client
     *
     * @return the client
     */
    ElasticsearchClient client();

    /**
     * Get index
     *
     * @return the index
     */
    String getIndex();

    /**
     * Set index
     *
     * @param index index
     * @return this search
     */
    Search setIndex(String index);

    /**
     * Create new search request
     *
     * @return this search request
     */
    BasicSearchRequest newSearchRequest();

    /**
     * Create new get request
     *
     * @return this search request
     */
    BasicGetRequest newGetRequest();

    /**
     * Shutdown and release all resources
     */
    void shutdown();

}

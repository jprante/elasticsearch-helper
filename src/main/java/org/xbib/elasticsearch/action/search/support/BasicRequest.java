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
package org.xbib.elasticsearch.action.search.support;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Helper class for Elasticsearch search/get requests
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class BasicRequest {

    private final ESLogger logger = ESLoggerFactory.getLogger(BasicRequest.class.getName());
    private SearchRequestBuilder searchRequestBuilder;
    private GetRequestBuilder getRequestBuilder;
    private String[] index;
    private String[] type;
    private String id;
    private String query;

    public BasicRequest newSearchRequest(SearchRequestBuilder searchRequestBuilder) {
        this.searchRequestBuilder = searchRequestBuilder;
        return this;
    }

    public SearchRequestBuilder searchRequestBuilder() {
        return searchRequestBuilder;
    }

    public BasicRequest newGetRequest(GetRequestBuilder getRequestBuilder) {
        this.getRequestBuilder = getRequestBuilder;
        return this;
    }

    public GetRequestBuilder getRequestBuilder() {
        return getRequestBuilder;
    }

    public BasicRequest index(String index) {
        if (index != null && !"*".equals(index)) {
            this.index = new String[]{index};
        }
        return this;
    }

    public BasicRequest index(String... index) {
        this.index = index;
        return this;
    }

    public String index() {
        return index[0];
    }

    public BasicRequest type(String type) {
        if (type != null && !"*".equals(type)) {
            this.type = new String[]{type};
        }
        return this;
    }

    public BasicRequest type(String... type) {
        this.type = type;
        return this;
    }

    public String type() {
        return type[0];
    }

    public BasicRequest id(String id) {
        this.id = id;
        return this;
    }

    public String id() {
        return id;
    }

    public BasicRequest from(int from) {
        searchRequestBuilder.setFrom(from);
        return this;
    }

    public BasicRequest size(int size) {
        searchRequestBuilder.setSize(size);
        return this;
    }

    public BasicRequest filter(String filter) {
        searchRequestBuilder.setFilter(filter);
        return this;
    }

    public BasicRequest facets(String facets) {
        searchRequestBuilder.setFacets(facets.getBytes());
        return this;
    }

    public BasicRequest timeout(TimeValue timeout) {
        searchRequestBuilder.setTimeout(timeout);
        return this;
    }

    public BasicRequest query(String query) {
        this.query = query == null || query.trim().length() == 0 ? "{\"query\":{\"match_all\":{}}}" : query;
        return this;
    }

    public BasicResponse execute()
            throws IOException {
        BasicResponse response = new BasicResponse();
        if (searchRequestBuilder == null) {
            return response;
        }
        if (query == null) {
            return response;
        }
        if (hasIndex(index)) {
            searchRequestBuilder.setIndices(fixIndexName(index));
        }
        if (hasType(type)) {
            searchRequestBuilder.setTypes(type);
        }
        long t0 = System.currentTimeMillis();
        response.searchResponse(searchRequestBuilder.setExtraSource(query)
                .execute().actionGet());
        long t1 = System.currentTimeMillis();
        logger.info(" [{}] [{}ms] [{}ms] [{}] [{}]",
                formatIndexType(), t1 - t0, response.tookInMillis(), response.totalHits(), query);
        return response;
    }

    public BasicResponse executeGet() throws IOException {
        BasicResponse response = new BasicResponse();
        long t0 = System.currentTimeMillis();
        response.getResponse(getRequestBuilder.execute().actionGet());
        long t1 = System.currentTimeMillis();
        logger.info(" get complete: {}/{}/{} [{}ms] {}",
                index, type, getRequestBuilder.request().id(), (t1 - t0), response.exists());
        return response;
    }

    private boolean hasIndex(String[] s) {
        if (s == null) {
            return false;
        }
        if (s.length == 0) {
            return false;
        }
        if (s[0] == null) {
            return false;
        }
        return true;
    }

    private boolean hasType(String[] s) {
        if (s == null) {
            return false;
        }
        if (s.length == 0) {
            return false;
        }
        if (s[0] == null) {
            return false;
        }
        return true;
    }

    private String[] fixIndexName(String[] s) {
        if (s == null) {
            return new String[]{"*"};
        }
        if (s.length == 0) {
            return new String[]{"*"};
        }
        for (int i = 0; i < s.length; i++) {
            if (s[i] == null || s[i].length() == 0) {
                s[i] = "*";
            }
        }
        return s;
    }

    private String formatIndexType() {
        StringBuilder indexes = new StringBuilder();
        if (index != null) {
            for (String s : index) {
                if (s != null && s.length() > 0) {
                    if (indexes.length() > 0) {
                        indexes.append(',');
                    }
                    indexes.append(s);
                }
            }
        }
        if (indexes.length() == 0) {
            indexes.append('*');
        }
        StringBuilder types = new StringBuilder();
        if (type != null) {
            for (String s : type) {
                if (s != null && s.length() > 0) {
                    if (types.length() > 0) {
                        types.append(',');
                    }
                    types.append(s);
                }
            }
        }
        if (types.length() == 0) {
            types.append('*');
        }
        return indexes.append("/").append(types).toString();
    }

}

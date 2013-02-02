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
package org.elasticsearch.action.search.support;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper class for ElasticsearchHelper responses
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class ElasticsearchResponse {

    private SearchResponse searchResponse;
    private GetResponse getResponse;

    public ElasticsearchResponse searchResponse(SearchResponse response) {
        this.searchResponse = response;
        return this;
    }

    public ElasticsearchResponse getResponse(GetResponse response) {
        this.getResponse = response;
        return this;
    }

    public long tookInMillis() {
        return searchResponse.tookInMillis();
    }

    public long totalHits() {
        return searchResponse.getHits().getTotalHits();
    }

    public boolean exists() {
        return getResponse.exists();
    }

    public ElasticsearchResponse toJson(OutputStream out) throws IOException {
        if (out == null) {
            return this;
        }
        if (searchResponse == null) {
            out.write(jsonErrorMessage("no response yet"));
            return this;
        }
        XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, out);
        jsonBuilder.startObject();
        searchResponse.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();
        jsonBuilder.close();
        return this;
    }

    private static byte[] jsonEmptyMessage(String message) {
        return ("{\"error\":404,\"message\":\"" + message + "\"}").getBytes();
    }


    private static byte[] jsonErrorMessage(String message) {
        return ("{\"error\":500,\"message\":\"" + message + "\"}").getBytes();
    }


}

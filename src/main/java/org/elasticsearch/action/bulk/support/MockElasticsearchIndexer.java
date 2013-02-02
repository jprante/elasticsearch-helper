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
package org.elasticsearch.action.bulk.support;

import org.elasticsearch.action.search.support.MockElasticsearch;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

/**
 * Elasticsearch Indexer Mockup
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class MockElasticsearchIndexer extends MockElasticsearch implements IElasticsearchIndexer {

    private String index;
    private String type;

    @Override
    public MockElasticsearchIndexer settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public MockElasticsearchIndexer newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockElasticsearchIndexer newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public MockElasticsearchIndexer dateDetection(boolean dateDetection) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer maxBulkActions(int maxBulkActions) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer waitForHealthyCluster() throws IOException {
        return this;
    }


    @Override
    public MockElasticsearchIndexer index(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String index() {
        return index;
    }

    @Override
    public MockElasticsearchIndexer type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public MockElasticsearchIndexer create(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockElasticsearchIndexer flush() {
        return this;
    }

    @Override
    public MockElasticsearchIndexer startBulkMode() {
        return this;
    }

    @Override
    public MockElasticsearchIndexer stopBulkMode() {
        return this;
    }

    @Override
    public long getVolumeInBytes() {
        return 0L;
    }

    @Override
    public MockElasticsearchIndexer deleteIndex() {
        return this;
    }

    @Override
    public MockElasticsearchIndexer newIndex() {
        return this;
    }
}

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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import org.xbib.elasticsearch.action.search.support.BasicRequest;
import org.xbib.elasticsearch.support.AbstractClient;

import java.net.URI;

/**
 * Search client support
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class SearchClientSupport extends AbstractClient implements SearchClient {

    protected Settings settings;

    private String index;

    private String type;

    public SearchClientSupport setIndex(String index) {
        this.index = index;
        return this;
    }

    public SearchClientSupport setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public SearchClientSupport newClient() {
        super.newClient();
        return this;
    }

    @Override
    public SearchClientSupport newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    public Client client() {
        return super.client();
    }

    /**
     * Create settings
     *
     * @param uri
     * @param n the client thread pool size
     * @return the settings
     */
    protected Settings initialSettings(URI uri, int n) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false) // sniff would join us into any cluster ... bug?
                .put("transport.netty.worker_count", n)
                .put("transport.netty.connections_per_node.low", 0)
                .put("transport.netty.connections_per_node.med", 0)
                .put("transport.netty.connections_per_node.high", n)
                .put("threadpool.index.type", "fixed")
                .put("threadpool.index.size", 1)
                .put("threadpool.bulk.type", "fixed")
                .put("threadpool.bulk.size", 1)
                .put("threadpool.get.type", "fixed")
                .put("threadpool.get.size", n)
                .put("threadpool.search.type", "fixed")
                .put("threadpool.search.size", n)
                .put("threadpool.percolate.type", "fixed")
                .put("threadpool.percolate.size", 1)
                .put("threadpool.management.type", "fixed")
                .put("threadpool.management.size", 1)
                .put("threadpool.flush.type", "fixed")
                .put("threadpool.flush.size", 1)
                .put("threadpool.merge.type", "fixed")
                .put("threadpool.merge.size", 1)
                .put("threadpool.refresh.type", "fixed")
                .put("threadpool.refresh.size", 1)
                .put("threadpool.cache.type", "fixed")
                .put("threadpool.cache.size", 1)
                .put("threadpool.snapshot.type", "fixed")
                .put("threadpool.snapshot.size", 1)
                .build();
    }

    @Override
    public BasicRequest newSearchRequest() {
        return new BasicRequest()
                .newSearchRequest(client.prepareSearch().setPreference("_primary_first"));
    }

    @Override
    public BasicRequest newGetRequest() {
        return new BasicRequest()
                .newGetRequest(client.prepareGet());
    }

}

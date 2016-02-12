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

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xbib.elasticsearch.action.search.helper.BasicGetRequest;
import org.xbib.elasticsearch.action.search.helper.BasicSearchRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Search client support
 */
public class SearchTransportClient extends BaseTransportClient implements Search {

    private String index;

    private String type;

    public String getIndex() {
        return index;
    }

    public SearchTransportClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public SearchTransportClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public SearchTransportClient init(Map<String, String> settings) throws IOException {
        init(Settings.builder().put(settings).build());
        return this;
    }

    @Override
    public SearchTransportClient init(Settings settings) throws IOException {
        super.createClient(settings);
        Collection<InetSocketTransportAddress> addrs = findAddresses(settings);
        if (!connect(addrs, settings.getAsBoolean("autodiscover", false))) {
            throw new NoNodeAvailableException("no cluster nodes available, check settings "
                    + settings.getAsMap());
        }
        return this;
    }

    public Client client() {
        return client;
    }

    @Override
    public BasicSearchRequest newSearchRequest() {
        return new BasicSearchRequest()
                .newRequest(client.prepareSearch());
    }

    @Override
    public BasicGetRequest newGetRequest() {
        return new BasicGetRequest()
                .newRequest(client.prepareGet());
    }

}

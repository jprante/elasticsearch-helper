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

import org.elasticsearch.client.support.ElasticsearchHelper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Elasticsearch mockup
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class MockElasticsearch extends ElasticsearchHelper {

    private final ESLogger logger = ESLoggerFactory.getLogger(MockElasticsearch.class.getName());
    protected Settings settings;
    private final Set<InetSocketTransportAddress> addresses = new HashSet();

    public MockElasticsearch() {
    }

    public MockElasticsearch settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * No special initial settings except cluster name
     *
     * @param uri
     * @return initial settings
     */
    @Override
    protected Settings initialSettings(URI uri) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .build();
    }

    @Override
    public MockElasticsearch index(String index) {
        super.index(index);
        return this;
    }

    public MockElasticsearch newClient() {
        return newClient(findURI());
    }

    public MockElasticsearch newClient(URI uri) {
        settings = initialSettings(uri);
        return this;
    }

    public synchronized void shutdown() {
    }

    public ElasticsearchRequest newRequest() {
        return new ElasticsearchRequest();
    }

    /**
     * Configure addresses, but do not connect.
     *
     * @param uri
     * @throws IOException
     */
    protected void connect(URI uri) throws IOException {
        String hostname = uri.getHost();
        int port = uri.getPort();
        if (!"es".equals(uri.getScheme())) {
            logger.warn("please specify URI scheme 'es'");
        }
        if ("hostname".equals(hostname)) {
            InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getLocalHost().getHostName(), port);
            if (!addresses.contains(address)) {
                logger.info("adding hostname address for transport client = {}", address);
                addresses.add(address);
            }
        } else if ("inet4".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    if (addr instanceof Inet4Address) {
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            logger.info("adding interface address for transport client = {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                        }
                    }
                }
            }
        } else if ("inet6".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    if (addr instanceof Inet6Address) {
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            logger.info("adding interface address for transport client = {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                        }
                    }
                }
            }
        } else {
            InetSocketTransportAddress address = new InetSocketTransportAddress(hostname, port);
            if (!addresses.contains(address)) {
                logger.info("adding custom address for transport client = {}", address);
                client.addTransportAddress(address);
                addresses.add(address);
            }
        }
        logger.info("configured addresses to connect = {}", addresses);
    }

    public MockElasticsearch waitForHealthyCluster() throws IOException {
        return this;
    }

}

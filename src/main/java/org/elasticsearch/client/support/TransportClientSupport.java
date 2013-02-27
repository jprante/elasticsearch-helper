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
package org.elasticsearch.client.support;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

public abstract class TransportClientSupport {

    private final static ESLogger logger = Loggers.getLogger(TransportClientSupport.class);

    // the default cluster name
    private final static String DEFAULT_CLUSTER_NAME = "elasticsearch";
    // the default connection specification
    private final static URI DEFAULT_URI = URI.create("es://hostname:9300");
    // the transport addresses
    private final Set<InetSocketTransportAddress> addresses = new HashSet();
    // singleton
    protected TransportClient client;
    // the settings
    protected Settings settings;
    // if the client has connectd nodes or not
    private boolean connected;

    public abstract String getIndex();

    protected abstract Settings initialSettings(URI uri);

    public TransportClientSupport newClient() {
        return newClient(findURI());
    }

    public synchronized TransportClientSupport newClient(URI uri) {
        if (client != null) {
            client.close();
            client = null;
        }
        if (client == null) {
            this.connected = false;
            this.settings = initialSettings(uri);
            logger.info("settings={}", settings.getAsMap());
            this.client = new TransportClient(settings);
            try {
                connect(uri);
                connected = !client.connectedNodes().isEmpty();
            } catch (UnknownHostException e) {
                logger.error(e.getMessage(), e);
            } catch (SocketException e) {
                logger.error(e.getMessage(), e);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return this;
    }

    public boolean isConnected() {
        return connected;
    }

    public synchronized void shutdown() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (addresses != null) {
            addresses.clear();
        }
    }

    protected static URI findURI() {
        URI uri = DEFAULT_URI;
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            logger.debug("the hostname is {}", hostname);
            URL url = TransportClientSupport.class.getResource("/org/xbib/elasticsearch/cluster.properties");
            if (url != null) {
                InputStream in = url.openStream();
                Properties p = new Properties();
                p.load(in);
                in.close();
                // the properties contains default URIs per hostname
                if (p.containsKey(hostname)) {
                    uri = URI.create(p.getProperty(hostname));
                    logger.debug("URI found in cluster.properties for hostname {}: {}", hostname, uri);
                    return uri;
                }
            }
        } catch (UnknownHostException e) {
            logger.warn("can't resolve host name, probably something wrong with network config: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        logger.debug("URI for hostname {}: {}", hostname, uri);
        return uri;
    }

    protected String findClusterName(URI uri) {
        String clustername;
        try {
            // look for URI parameters
            Map<String, String> map = parseQueryString(uri, "UTF-8");
            clustername = map.get("es.cluster.name");
            if (clustername != null) {
                logger.info("cluster name found in URI {}: {}", uri, clustername);
                return clustername;
            }
            clustername = map.get("cluster.name");
            if (clustername != null) {
                logger.info("cluster name found in URI {}: {}", uri, clustername);
                return clustername;
            }
        } catch (UnsupportedEncodingException ex) {
            logger.warn(ex.getMessage(), ex);
        }
        logger.info("cluster name not found in URI {}, parameter es.cluster.name", uri);
        clustername = System.getProperty("es.cluster.name");
        if (clustername != null) {
            logger.info("cluster name found in es.cluster.name system property: {}", clustername);
            return clustername;
        }
        clustername = System.getProperty("cluster.name");
        if (clustername != null) {
            logger.info("cluster name found in cluster.name system property: {}", clustername);
            return clustername;
        }
        logger.info("cluster name not found, falling back to default: {}", DEFAULT_CLUSTER_NAME);
        clustername = DEFAULT_CLUSTER_NAME;
        return clustername;
    }

    protected void connect(URI uri) throws IOException {
        String hostname = uri.getHost();
        int port = uri.getPort();
        boolean newaddresses = false;
        if (!"es".equals(uri.getScheme())) {
            logger.warn("please specify URI scheme 'es'");
        }
        if ("hostname".equals(hostname)) {
            InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getLocalHost().getHostName(), port);
            if (!addresses.contains(address)) {
                logger.info("adding hostname address for transport client: {}", address);
                client.addTransportAddress(address);
                addresses.add(address);
                newaddresses = true;
            }
        } else if ("inet4".equals(hostname)) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress addr : Collections.list(inetAddresses)) {
                    if (addr instanceof Inet4Address) {
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            logger.info("adding interface address for transport client: {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                            newaddresses = true;
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
                            logger.info("adding interface address for transport client: {}", address);
                            client.addTransportAddress(address);
                            addresses.add(address);
                            newaddresses = true;
                        }
                    }
                }
            }
        } else {
            InetSocketTransportAddress address = new InetSocketTransportAddress(hostname, port);
            if (!addresses.contains(address)) {
                logger.info("adding custom address for transport client: {}", address);
                client.addTransportAddress(address);
                addresses.add(address);
                newaddresses = true;
            }
        }
        logger.info("configured addresses to connect: {}", addresses);
        if (newaddresses) {
            List<DiscoveryNode> nodes = client.connectedNodes().asList();
            logger.info("connected nodes = {}", nodes);
            for (DiscoveryNode node : nodes) {
                logger.info("new connection to {} {}", node.getId(), node.getName());
            }
        }
    }

    public TransportClientSupport waitForHealthyCluster() throws IOException {
        return waitForHealthyCluster(ClusterHealthStatus.YELLOW, "30s");
    }

    public TransportClientSupport waitForHealthyCluster(ClusterHealthStatus status, String timeout) throws IOException {
        try {
            logger.info("waiting for cluster health...");
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster not healthy, cowardly refusing to continue with operations: " + status.name());
            } else {
                logger.info("... cluster is healthy");
            }
        } catch (ElasticSearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
        return this;
    }

    public int waitForRecovery() {
        if (getIndex() == null) {
            return -1;
        }
        IndicesStatusResponse response = client.admin().indices()
                .status(new IndicesStatusRequest(getIndex()).recovery(true)).actionGet();
        return response.totalShards();
    }

    protected TransportClientSupport enableRefreshInterval() {
        update("refresh_interval", 1000);
        return this;
    }

    protected TransportClientSupport disableRefreshInterval() {
        update("refresh_interval", -1);
        return this;
    }

    protected void update(String key, Object value) {
        if (client == null) {
            return;
        }
        if (value == null) {
            return;
        }
        if (getIndex() == null) {
            return;
        }
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(getIndex())
                .settings(settingsBuilder);
        client.admin().indices().updateSettings(updateSettingsRequest).actionGet();
    }

    private Map<String, String> parseQueryString(URI uri, String encoding)
            throws UnsupportedEncodingException {
        Map<String, String> m = new HashMap<String, String>();
        if (uri == null) {
            throw new IllegalArgumentException();
        }
        if (uri.getRawQuery() == null) {
            return m;
        }
        // use getRawQuery because we do our decoding by ourselves
        StringTokenizer st = new StringTokenizer(uri.getRawQuery(), "&");
        while (st.hasMoreTokens()) {
            String pair = st.nextToken();
            int pos = pair.indexOf('=');
            if (pos < 0) {
                m.put(pair, null);
            } else {
                m.put(pair.substring(0, pos), decode(pair.substring(pos + 1, pair.length()), encoding));
            }
        }
        return m;
    }

    private String decode(String s, String encoding) {
        StringBuilder sb = new StringBuilder();
        boolean fragment = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '+':
                    sb.append(' ');
                    break;
                case '#':
                    sb.append(ch);
                    fragment = true;
                    break;
                case '%':
                    if (!fragment) {
                        // fast hex decode
                        sb.append((char) ((Character.digit(s.charAt(++i), 16) << 4)
                                | Character.digit(s.charAt(++i), 16)));
                    } else {
                        sb.append(ch);
                    }
                    break;
                default:
                    sb.append(ch);
                    break;
            }
        }
        try {
            // URL default encoding is ISO-8859-1
            return new String(sb.toString().getBytes("ISO-8859-1"), encoding);
        } catch (UnsupportedEncodingException e) {
            throw new Error("encoding " + encoding + " not supported");
        }
    }
}

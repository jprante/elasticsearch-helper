
package org.xbib.elasticsearch.support.helper;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.junit.After;
import org.junit.Before;

public abstract class AbstractNodeTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger("test");

    protected final String INDEX = "my_index";

    protected final String TYPE = "my_type";

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    private String host;

    private int port;

    protected URI getAddress() {
        return URI.create("es://" + getHost() + ":" + getPort() + "?es.cluster.name=" + getClusterName());
    }

    protected String getClusterName() {
        return "test-support-cluster-" + NetworkUtils.getLocalAddress().getHostName();
    }

    protected String getHost() {
        return host;
    }

    protected int getPort() {
        return port;
    }

    protected Settings getNodeSettings() {
        return ImmutableSettings
                .settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replica", 0)
                .put("cluster.routing.schedule", "50ms")
                .put("gateway.type", "none")
                .put("index.store.type", "ram")
                .put("http.enabled", false)
                .put("discovery.zen.multicast.enabled", false)
                .put("threadpool.bulk.queue_size", 200)
                .build();
    }

    @Before
    public void startNodes() throws Exception {
        startNode("1");
        // find node address
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Object obj = response.iterator().next().getTransport().getAddress()
                .publishAddress();
        if (obj instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
            host = address.address().getHostName();
            port = address.address().getPort();
        }
        createIndices();
    }

    private void createIndices() throws Exception {
        logger.info("creating index {}", INDEX);
        try {
            client("1").admin().indices().create(new CreateIndexRequest(INDEX)).actionGet();
        } catch (IndexAlreadyExistsException e) {
            // ignore
        }
        logger.info("index {} created", INDEX);
    }

    @After
    public void stopNodes() throws Exception {
        deleteIndices();
        closeAllNodes();
    }

    private void deleteIndices() throws Exception {
        logger.info("deleting index {}", INDEX);
        try {
            client("1").admin().indices().delete(new DeleteIndexRequest().indices(INDEX)).actionGet();
        } catch (IndexMissingException e) {
            // ignore
        }
        logger.info("index {} deleted", INDEX);
        closeAllNodes();
    }

    protected Node startNode(String id) {
        return buildNode(id).start();
    }

    private Node buildNode(String id) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(getNodeSettings())
                .put("name", id)
                .build();
        Node node = nodeBuilder().settings(finalSettings).build();
        Client client = node.client();
        nodes.put(id, node);
        clients.put(id, client);
        return node;
    }

    protected void stopNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        nodes.clear();
    }

}

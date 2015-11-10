package org.xbib.elasticsearch.helper.helper;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.junit.After;
import org.junit.Before;
import org.xbib.elasticsearch.helper.client.ClientHelper;

public abstract class AbstractNodeTestHelper {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    private AtomicInteger counter = new AtomicInteger();

    private String cluster;

    private String host;

    private int port;

    protected void setClusterName() {
        this.cluster = "test-support-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return cluster;
    }

    protected String getHome() {
        return System.getProperty("path.home");
    }

    protected Settings getSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("host", host)
                .put("port", port)
                .put("cluster.name", cluster)
                .build();
    }

    protected Settings getNodeSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", cluster)
                .put("cluster.routing.schedule", "50ms") // for replica update tests
                .put("cluster.routing.allocation.disk.threshold_enabled", false) // for replica tests
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("http.enabled", false)
                .put("discovery.zen.multicast.enabled", true) // for multi node start
                .put("threadpool.bulk.size", Runtime.getRuntime().availableProcessors())
                .put("threadpool.bulk.queue_size", 20 * Runtime.getRuntime().availableProcessors() ) // default is 50, which is too low for our tests
                .put("path.home", getHome())
                .build();
    }

    @Before
    public void startNodes() throws Exception {
        setClusterName();
        startNode("1");
        findNodeAddress();
        ClientHelper.waitForCluster(client("1"), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30));
        logger.info("ready");
    }

    @After
    public void stopNodes() throws Exception {
        try {
            logger.info("deleting all indices");
            // delete all indices
            client("1").admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (Exception e) {
            logger.error("can not delete indexes", e);
        } finally {
            closeAllNodes();
        }
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
        logger.info("settings={}", finalSettings.getAsMap());
        Node node = nodeBuilder()
                .settings(finalSettings).build();
        Client client = node.client();
        nodes.put(id, node);
        clients.put(id, client);
        return node;
    }

    public Client client(String id) {
        return clients.get(id);
    }

    protected void findNodeAddress() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Object obj = response.iterator().next().getTransport().getAddress()
                .publishAddress();
        if (obj instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
            host = address.address().getHostName();
            port = address.address().getPort();
        }
    }

    public void closeAllNodes() throws IOException {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        logger.info("all nodes closed");
        nodes.clear();
        deleteFiles();
        logger.info("data files wiped");
    }

    private void deleteFiles() throws IOException {
        Path directory = Paths.get(getHome() + "/data");
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });

    }
}

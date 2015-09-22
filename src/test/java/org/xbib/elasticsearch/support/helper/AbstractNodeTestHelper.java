package org.xbib.elasticsearch.support.helper;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.junit.After;
import org.junit.Before;
import org.xbib.elasticsearch.plugin.support.SupportPlugin;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.network.NetworkUtils;

public abstract class AbstractNodeTestHelper {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    private String cluster;

    private String host;

    private int port;

    protected void setClusterName() {
        this.cluster = "test-support-cluster-"
                + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return cluster;
    }

    protected String getHome() {
        return System.getProperty("path.home");
    }

    protected Settings getSettings() {
        return settingsBuilder()
                .put("host", host)
                .put("port", port)
                .put("cluster.name", cluster)
                .put("path.home", getHome())
                .put("plugin.types", SupportPlugin.class.getName())
                .build();
    }

    protected Settings getNodeSettings() {
        return settingsBuilder()
                .put("cluster.name", cluster)
                .put("cluster.routing.schedule", "50ms")
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.multicast.ping_timeout", "5s")
                .put("http.enabled", false)
                .put("threadpool.bulk.size", Runtime.getRuntime().availableProcessors())
                .put("threadpool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors()) // default is 50, too low
                .put("index.number_of_replicas", 0)
                .put("path.home", getHome())
                .put("plugin.types", SupportPlugin.class.getName())
                .build();
    }

    @Before
    public void startNodes() {
        try {
            setClusterName();
            startNode("1");
            findNodeAddress();
            ClientHelper.waitForCluster(client("1"), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30));
            logger.info("ready");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @After
    public void stopNodes() {
        try {
            //logger.info("deleting all indices");
            // delete all indices
            //client("1").admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (Exception e) {
            logger.error("can not delete indexes", e);
        } finally {
            try {
                closeAllNodes();
            } catch (Throwable t) {
                logger.error("close all nodes failed", t);
            }
        }
    }

    protected Node startNode(String id) throws IOException {
        return buildNode(id).start();
    }

    public AbstractClient client(String id) {
        return clients.get(id);
    }

    private Node buildNode(String id) throws IOException {
        Settings finalSettings = settingsBuilder()
                .put(getNodeSettings())
                .put("name", id)
                .build();
        logger.info("settings={}", finalSettings.getAsMap());
        Node node = nodeBuilder().settings(finalSettings).build();
        AbstractClient client = (AbstractClient)node.client();
        nodes.put(id, node);
        clients.put(id, client);
        logger.info("clients={}", clients);
        return node;
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
        logger.info("closing all clients");
        for (AbstractClient client : clients.values()) {
            client.close();
        }
        clients.clear();
        logger.info("closing all nodes");
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        nodes.clear();
        logger.info("all nodes closed");
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

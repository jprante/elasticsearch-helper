package org.xbib.elasticsearch.support.helper;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Random;
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
import org.xbib.elasticsearch.support.client.ClientHelper;

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
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (host != null) {
            builder.put("host", host);
            builder.put("port", port);
        }
        builder.put("cluster.name", cluster);
        return builder.build();
    }

    protected Settings getNodeSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", cluster)
                .put("discovery.zen.multicast.enabled", true)
                .put("http.enabled", false)
                .put("index.number_of_replicas", 0)
                .put("threadpool.bulk.queue_size", 10 * Runtime.getRuntime().availableProcessors()) // default is 50, too low
                .build();
    }

    @Before
    public void startNodes() {
        try {
            boolean b = checkFiles();
            if (b) {
                throw new IOException("data files exists, quitting");
            }
            setClusterName();
            startNode("1");
            // find node address
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
            NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
            Object obj = response.iterator().next().getTransport().getAddress().publishAddress();
            if (obj instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
                host = address.address().getHostName();
                port = address.address().getPort();
            }
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    protected void waitForCluster() throws IOException {
        ClientHelper.waitForCluster(client("1"), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30));
    }

    @After
    public void stopNodes() {
        try {
            logger.info("stopping nodes");
        } finally {
            logger.info("closing nodes");
            try {
                closeAllNodes();
            } catch (Throwable t) {
                logger.error("stopNodes failed", t);
            } finally {
                try {
                    deleteFiles();
                    logger.info("files deleted");
                } catch (Throwable t2) {
                    // ignore
                } finally {
                    try {
                        boolean b = checkFiles();
                        if (b) {
                            logger.error("data files exists??");
                        } else {
                            logger.info("files gone");
                        }
                    } catch (Throwable t3) {
                        //
                    }
                }
            }
        }
    }

    protected Node startNode(String id) {
        return buildNode(id).start();
    }

    private Node buildNode(String id) {
        Settings finalSettings = settingsBuilder()
                .put(getNodeSettings())
                .put("name", id)
                .build();
        logger.info("settings={}", finalSettings.getAsMap());
        Node node = nodeBuilder().settings(finalSettings).build();
        Client client = node.client();
        nodes.put(id, node);
        clients.put(id, client);
        return node;
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() throws IOException {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            if (node != null) {
                node.stop();
            }
        }
        nodes.clear();
        try {
            Thread.sleep(2000L);
        } catch (Throwable t) {
            //
        }
    }

    private boolean checkFiles() throws IOException {
        Path directory = Paths.get(getHome() + "/data").toAbsolutePath();
        boolean b = directory.toFile().exists() &&
                directory.toFile().isDirectory();
        final AtomicInteger count = new AtomicInteger();
        if (b) {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    count.incrementAndGet();
                    return FileVisitResult.CONTINUE;
                }
            });
            b = count.get() == 0;
        }
        return b;
    }

    private void deleteFiles() throws IOException {
        Path directory = Paths.get(getHome() + "/data").toAbsolutePath();
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


    private static Random random = new Random();

    private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();

    protected String randomString(int len) {
        final char[] buf = new char[len];
        final int n = numbersAndLetters.length - 1;
        for (int i = 0; i < buf.length; i++) {
            buf[i] = numbersAndLetters[random.nextInt(n)];
        }
        return new String(buf);
    }

}

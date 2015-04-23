
package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestTransportClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClientTest.class.getSimpleName());

    @Test
    public void testNewIndexIngest() throws IOException {
        final IngestTransportClient ingest = new IngestTransportClient()
                .newClient(getSettings())
                .shards(2)
                .replica(0)
                .newIndex("test");
        ingest.shutdown();
        if (ingest.hasThrowable()) {
            logger.error("error", ingest.getThrowable());
        }
        assertFalse(ingest.hasThrowable());
    }

    @Test
    public void testDeleteIndexIngestClient() throws IOException {
        final IngestTransportClient ingest = new IngestTransportClient()
                .newClient(getSettings())
                .shards(2)
                .replica(0)
                .newIndex("test");
        try {
            ingest.deleteIndex("test")
              .newIndex("test")
              .deleteIndex("test");
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            ingest.shutdown();
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
        }
    }

    @Test
    public void testSingleDocIngestClient() throws IOException {
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .newClient(getSettings())
                .shards(2)
                .replica(0)
                .newIndex("test");
        try {
            ingest.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (InterruptedException e) {
            // ignore
        } finally {
            logger.info("total bulk requests = {}", ingest.getMetric().getTotalIngest().count());
            assertEquals(1, ingest.getMetric().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.shutdown();
        }
    }

    @Test
    public void testRandomDocsIngestClient() throws Exception {
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerBulkRequest(1000)
                .newClient(getSettings())
                .shards(2)
                .replica(0)
                .newIndex("test")
                .startBulk("test", -1, 1000);
        try {
            for (int i = 0; i < 12345; i++) {
                ingest.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (InterruptedException e) {
            // ignore
        } finally {
            ingest.stopBulk("test");
            logger.info("total requests = {}", ingest.getMetric().getTotalIngest().count());
            assertEquals(13, ingest.getMetric().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocsIngestClient() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        int maxactions = 1000;
        final int maxloop = 12345;
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerBulkRequest(maxactions)
                .newClient(getSettings())
                .shards(2)
                .replica(0)
                .newIndex("test")
                .startBulk("test", -1, 1000);
        try {
            ThreadPoolExecutor pool = EsExecutors.newFixed(maxthreads, 30,
                    EsExecutors.daemonThreadFactory("ingest-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            ingest.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 60 seconds...");
            latch.await(60, TimeUnit.SECONDS);
            logger.info("client flush ...");
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("thread pool to be shut down ...");
            pool.shutdown();
            logger.info("thread pool shut down");
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            ingest.stopBulk("test");
            logger.info("total requests = {}", ingest.getMetric().getTotalIngest().count());
            assertEquals(maxthreads * maxloop / maxactions + 1, ingest.getMetric().getTotalIngest().count());
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
            ingest.refresh("test");
            assertEquals(maxthreads * maxloop,
                    ingest.client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount()
            );
            ingest.shutdown();
        }
    }

    @Test
    public void testClusterConnect() throws IOException {
        startNode("2");
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
        settingsBuilder.put("cluster.name", getClusterName());
        settingsBuilder.put("autodiscover", true);
        int i = 0;
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        for (NodeInfo nodeInfo : response) {
            TransportAddress ta = nodeInfo.getTransport().getAddress().publishAddress();
            if (ta instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress address = (InetSocketTransportAddress) ta;
                settingsBuilder.put("host." + i++, address.address().getHostName() + ":" + address.address().getPort());
            }
        }
        final IngestTransportClient ingest = new IngestTransportClient()
                .newClient(settingsBuilder.build())
                .newIndex("test");
        ingest.shutdown();
        if (ingest.hasThrowable()) {
            logger.error("error", ingest.getThrowable());
        }
        assertFalse(ingest.hasThrowable());
    }

}

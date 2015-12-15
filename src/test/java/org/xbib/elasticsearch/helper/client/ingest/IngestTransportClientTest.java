
package org.xbib.elasticsearch.helper.client.ingest;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.xbib.elasticsearch.helper.client.ClientHelper;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.xbib.elasticsearch.util.NodeTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestTransportClientTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(IngestTransportClientTest.class.getSimpleName());

    private final static Integer MAX_ACTIONS = 1000;

    private final static Integer NUM_ACTIONS = 12345;

    @Before
    public void startNodes() {
        try {
            super.startNodes();
            startNode("2");
            startNode("3");
            logger.info("started 3 nodes");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    // disabled
    public void testNewIndexIngest() throws IOException {
        Settings settingsForIndex = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient()
                .init(getSettings(), new LongAdderIngestMetric())
                .newIndex("test", settingsForIndex, null);
        ingest.shutdown();
        if (ingest.hasThrowable()) {
            logger.error("error", ingest.getThrowable());
        }
        assertFalse(ingest.hasThrowable());
    }

    // disabled
    public void testDeleteIndexIngestClient() throws IOException {
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient()
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            ingest.newIndex("test", settings, null);
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
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            ingest.newIndex("test", settings, null);
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
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerRequest(MAX_ACTIONS)
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            ingest.newIndex("test", settings, null)
                    .startBulk("test", -1, 1000);
            for (int i = 0; i < NUM_ACTIONS; i++) {
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
            assertEquals(NUM_ACTIONS / MAX_ACTIONS + 1, ingest.getMetric().getTotalIngest().count());
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
        int maxactions = MAX_ACTIONS;
        final int maxloop = NUM_ACTIONS;
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient()
                .flushIngestInterval(TimeValue.timeValueSeconds(600))
                .maxActionsPerRequest(maxactions)
                .init(getSettings(), new LongAdderIngestMetric());
        try {
            ClientHelper.waitForCluster(ingest.client(), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(10));
            ingest.newIndex("test", settings, null)
                    .startBulk("test", -1, 1000);
            ThreadPoolExecutor pool =
                    EsExecutors.newFixed("ingestclient-test", maxthreads, 30, EsExecutors.daemonThreadFactory("ingestclient-test"));
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
            ingest.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(ingest.client(), SearchAction.INSTANCE)
                    .setIndices("_all") // to avoid NPE
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
            ingest.shutdown();
        }
    }
}

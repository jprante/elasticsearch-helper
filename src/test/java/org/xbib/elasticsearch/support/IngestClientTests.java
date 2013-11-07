
package org.xbib.elasticsearch.support;

import org.xbib.elasticsearch.support.client.IngestClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class IngestClientTests extends AbstractNodeTest {

    private final static ESLogger logger = Loggers.getLogger(IngestClientTests.class);

    @Test
    public void testnewIndex() {
        final IngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("test")
                .setType("test")
                .newIndex();
        es.shutdown();
    }

    @Test
    public void testDeleteIndex() {
        final IngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("test")
                .setType("test")
                .newIndex();
        logger.info("transport client up");
        try {
            es.deleteIndex()
              .newIndex()
              .deleteIndex();
        } catch (NoNodeAvailableException e) {
            logger.error("no node available");
        } finally {
            es.shutdown();
            logger.info("transport client down");
        }
    }

    @Test
    public void testSingleDocIngest() {
        final IngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            es.deleteIndex();
            es.newIndex();
            es.indexDocument("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}"); // single doc ingest
            es.flush();
            logger.info("stats={}", es.stats());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testRandomIngest() {
        final IngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            for (int i = 0; i < 12345; i++) {
                es.indexDocument("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testThreadedRandomIngest() throws Exception {
        final IngestClient es = new IngestClient()
                .newClient(ADDRESS)
                .setIndex("test")
                .setType("test")
                .newIndex();
        try {
            int min = 0;
            int max = 4;
            ThreadPoolExecutor pool = EsExecutors.newScaling(min, max, 100, TimeUnit.DAYS,
                    EsExecutors.daemonThreadFactory("ingest"));
            final CountDownLatch latch = new CountDownLatch(max);
            for (int i = 0; i < max; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                            for (int i = 0; i < 12345; i++) {
                                es.indexDocument("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                            }
                            logger.info("done");
                            latch.countDown();
                    }
                });
            }
            logger.info("waiting for 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            pool.shutdown();
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }

    }

}

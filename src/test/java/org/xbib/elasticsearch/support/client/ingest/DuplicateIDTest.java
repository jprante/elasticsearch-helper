package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DuplicateIDTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(DuplicateIDTest.class.getSimpleName());

    @Test
    public void testDuplicateDocIDs() {
        final IngestTransportClient es = new IngestTransportClient()
                .maxActionsPerBulkRequest(1000)
                .newClient(getAddress())
                .newIndex("test");
        try {
            for (int i = 0; i < 12345; i++) {
                es.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            es.refresh("test");
            long hits = es.client().prepareSearch("test").setTypes("test")
                    .setQuery(matchAllQuery())
                    .execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < 12345);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            assertEquals(es.getState().getTotalIngest().count(), 13);
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }
}

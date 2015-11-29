package org.xbib.elasticsearch.helper.client.transport;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.util.NodeTestUtils;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BulkTransportDuplicateIDTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportDuplicateIDTest.class.getSimpleName());

    private final static Integer MAX_ACTIONS = 1000;

    private final static Integer NUM_ACTIONS = 12345;

    @Test
    public void testDuplicateDocIDs() throws Exception {
        final BulkTransportClient client = new BulkTransportClient()
                .maxActionsPerRequest(MAX_ACTIONS)
                .init(getSettings(), new LongAdderIngestMetric())
                .newIndex("test");
        try {
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setTypes("test")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < NUM_ACTIONS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            assertEquals(NUM_ACTIONS / MAX_ACTIONS + 1, client.getMetric().getTotalIngest().count());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}


package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.client.transport.NoNodeAvailableException;

import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NodeClientTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(NodeClientTest.class.getSimpleName());

    @Test
    public void testNewIndexNodeClient() throws Exception {
        final NodeClient es = new NodeClient()
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(client("1"))
                .setIndex(INDEX)
                .setType("test")
                .newIndex();
        es.shutdown();
        if (es.hasThrowable()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasThrowable());
    }

    @Test
    public void testMappingNodeClient() throws Exception {
        final NodeClient es = new NodeClient()
                .flushInterval(TimeValue.timeValueSeconds(5))
                .newClient(client("1"))
                .mapping("test", "{\"test\":{\"properties\":{\"location\":{\"type\":\"geo_point\"}} }")
                .setIndex(INDEX)
                .setType("test")
                .newIndex();

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest()
                .indices(INDEX);
        GetMappingsResponse getMappingsResponse = es.client().admin().indices().getMappings(getMappingsRequest).actionGet();

        logger.info("mappings={}", getMappingsResponse.getMappings());

        es.shutdown();
        if (es.hasThrowable()) {
            logger.error("error", es.getThrowable());
        }
        assertFalse(es.hasThrowable());
    }

    @Test
    public void testRandomDocsNodeClient() throws Exception {
        final NodeClient es = new NodeClient()
                .maxActionsPerBulkRequest(1000)
                .flushInterval(TimeValue.timeValueSeconds(10))
                .newClient(client("1"))
                .setIndex(INDEX)
                .setType("test")
                .newIndex();

        try {
            for (int i = 0; i < 12345; i++) {
                es.index(INDEX, "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
            assertEquals(13, es.getState().getTotalIngest().count());
            if (es.hasThrowable()) {
                logger.error("error", es.getThrowable());
            }
            assertFalse(es.hasThrowable());
        }
    }

}

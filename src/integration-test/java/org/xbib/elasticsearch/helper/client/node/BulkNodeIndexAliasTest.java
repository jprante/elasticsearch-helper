package org.xbib.elasticsearch.helper.client.node;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;
import org.xbib.elasticsearch.helper.client.BulkNodeClient;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.IndexAliasAdder;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public class BulkNodeIndexAliasTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeIndexAliasTest.class.getSimpleName());

    @Test
    public void testIndexAlias() throws Exception {
        final BulkNodeClient client = ClientBuilder.builder()
                .setMetric(new LongAdderIngestMetric())
                .toBulkNodeClient(client("1"));
        try {
            client.newIndex("test1234");
            for (int i = 0; i < 1; i++) {
                client.index("test1234", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test1234");

            List<String> simpleAliases = Arrays.asList("a", "b", "c");
            client.switchAliases("test", "test1234", simpleAliases);

            client.newIndex("test5678");
            for (int i = 0; i < 1; i++) {
                client.index("test5678", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.refreshIndex("test5678");

            simpleAliases = Arrays.asList("d", "e", "f");
            client.switchAliases("test", "test5678", simpleAliases, new IndexAliasAdder() {
                @Override
                public void addIndexAlias(IndicesAliasesRequestBuilder builder, String index, String alias) {
                    builder.addAlias(index, alias, QueryBuilders.termQuery("my_key", alias));
                }
            });
            Map<String, String> aliases = client.getIndexFilters("test5678");
            logger.info("aliases of index test5678 = {}", aliases);

            aliases = client.getAliasFilters("test");
            logger.info("aliases of alias test = {}", aliases);

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            client.shutdown();
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}

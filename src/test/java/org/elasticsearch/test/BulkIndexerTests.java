package org.elasticsearch.test;

import org.elasticsearch.action.bulk.support.ElasticsearchIndexer;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Test;

public class BulkIndexerTests {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkIndexerTests.class.getName());


    @Test
    public void testDeleteIndex() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch").build();

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(settings)
                .newClient()
                .index("test")
                .type("test");

        try {
            es.deleteIndex();
            es.index();
            es.deleteIndex();
        } catch (NoNodeAvailableException e) {
            // if no node, just skip
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testSimpleBulk() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch").build();

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(settings)
                .newClient()
                .index("test")
                .type("test");
        try {
            es.deleteIndex();
            es.index();
            es.index("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}");
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }

    }
}

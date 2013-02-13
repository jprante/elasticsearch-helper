package org.elasticsearch.test;

import org.elasticsearch.action.bulk.support.ElasticsearchIndexer;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Test;

import java.util.Random;

public class BulkIndexerTests extends AbstractNodeTest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkIndexerTests.class.getName());

    private Client client;

    @Test
    public void testDeleteIndex() throws Exception {

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
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

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
                .newClient()
                .index("test")
                .type("test");
        try {
            es.deleteIndex();
            es.index(); // create index
            es.index("test", "test", "1", "{ \"name\" : \"Jörg Prante\"}"); // single doc bulk
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testRandomBulk() {

        final ElasticsearchIndexer es = new ElasticsearchIndexer()
                .settings(defaultSettings)
                .newClient();
        try {
            for (int i = 0; i < 10000; i++) {
                es.index("test", "test", null, "{ \"name\" : \""+randomString(32)+"\"}");
            }
            es.flush();
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            es.shutdown();
        }
    }

    private static Random random = new Random();
    private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz").toCharArray();

    private String randomString(int len) {
        final char[] buf = new char[len];
        final int n = numbersAndLetters.length - 1;
        for (int i = 0; i < buf.length; i++) {
            buf[i] = numbersAndLetters[random.nextInt(n)];
        }
        return new String(buf);
    }

}

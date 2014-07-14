package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.xbib.elasticsearch.support.helper.AbstractNodeRandomTestHelper;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BulkReplicaTest extends AbstractNodeRandomTestHelper {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkReplicaTest.class.getSimpleName());

    @Test
    public void testReplicaLevel() throws Exception {

        // we need nodes for replica levels
        startNode("2");
        startNode("3");
        //startNode("4");

        final BulkTransportClient ingest = new BulkTransportClient()
                .newClient(getAddress())
                .shards(2)
                .replica(3)
                .newIndex("test1")
                .shards(2)
                .replica(1)
                .newIndex("test2");
        try {
            for (int i = 0; i < 1234; i++) {
                ingest.index("test1", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            for (int i = 0; i < 1234; i++) {
                ingest.index("test2", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            ingest.flushIngest();
            ingest.waitForResponses(TimeValue.timeValueSeconds(60));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            logger.info("refreshing");
            ingest.refresh("test1");
            ingest.refresh("test2");
            long hits = ingest.client().prepareSearch("test1","test2")
                    .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits();
            logger.info("query total hits={}", hits);
            assertEquals(2468, hits);
            IndicesStatsResponse response = ingest.client().admin().indices().prepareStats().all().execute().actionGet();
            for (Map.Entry<String,IndexStats> m : response.getIndices().entrySet()) {
                IndexStats indexStats = m.getValue();
                CommonStats commonStats = indexStats.getTotal();
                IndexingStats indexingStats = commonStats.getIndexing();
                IndexingStats.Stats stats = indexingStats.getTotal();
                logger.info("index {}: count = {}", m.getKey(), stats.getIndexCount());
                for (Map.Entry<Integer,IndexShardStats> me : indexStats.getIndexShards().entrySet()) {
                    IndexShardStats indexShardStats = me.getValue();
                    CommonStats commonShardStats = indexShardStats.getTotal();
                    logger.info("shard {} count = {}", me.getKey(),
                            commonShardStats.getIndexing().getTotal().getIndexCount());
                }
            }
            try {
                ingest.deleteIndex("test1")
                        .deleteIndex("test2");
            } catch (Exception e) {
                logger.error("delete index failed, ignored. Reason:", e);
            }
            ingest.shutdown();
            if (ingest.hasThrowable()) {
                logger.error("error", ingest.getThrowable());
            }
            assertFalse(ingest.hasThrowable());
        }

        //stopNode("4");
        stopNode("3");
        stopNode("2");
    }

}

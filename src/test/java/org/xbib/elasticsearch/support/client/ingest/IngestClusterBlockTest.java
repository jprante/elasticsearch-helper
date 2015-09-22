package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.junit.Before;
import org.junit.Test;

import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestRequestBuilder;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IngestClusterBlockTest extends AbstractNodeTestHelper {

    @Before
    public void startNodes() {
        try {
            setClusterName();
            startNode("1");
            findNodeAddress();
            // do not wait for green health state
            logger.info("ready");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    protected Settings getNodeSettings() {
        return Settings.settingsBuilder()
                .put(super.getNodeSettings())
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
    }

    @Test(expected = ClusterBlockException.class)
    public void testClusterBlockNodeClient() throws Exception {
        IngestRequestBuilder brb = client("1").prepareExecute(IngestAction.INSTANCE);
        XContentBuilder builder = jsonBuilder().startObject().field("field", "value").endObject();
        String jsonString = builder.string();
        IndexRequest indexRequest = client("1").prepareExecute(IndexAction.INSTANCE)
                .setIndex("test").setType("test").setId("1").setSource(jsonString).request();
        brb.add(indexRequest);
        brb.execute().actionGet();
    }

    @Test(expected = MasterNotDiscoveredException.class)
    public void testClusterBlockTransportClient() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 3)
                .build();
        final IngestTransportClient ingest = new IngestTransportClient();
        try {
            ingest.init(getSettings())
                    .newIndex("test", settings, null);
            IngestRequestBuilder brb = ingest.client().prepareExecute(IngestAction.INSTANCE);
            XContentBuilder builder = jsonBuilder().startObject().field("field", "bvalue").endObject();
            String jsonString = builder.string();
            IndexRequest indexRequest = ingest.client().prepareExecute(IndexAction.INSTANCE)
                    .setIndex("test").setType("test").setId("1").setSource(jsonString).request();
            brb.add(indexRequest);
            brb.execute().actionGet();
        } finally {
            ingest.shutdown();
        }
    }
}

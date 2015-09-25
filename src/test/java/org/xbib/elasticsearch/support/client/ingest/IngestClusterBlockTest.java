package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.junit.Test;

import org.xbib.elasticsearch.action.index.IndexAction;
import org.xbib.elasticsearch.action.index.IndexRequestBuilder;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.IngestRequestBuilder;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IngestClusterBlockTest extends AbstractNodeTestHelper {

    protected Settings getNodeSettings() {
        return ImmutableSettings
                .settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("http.enabled", false)
                .put("index.number_of_replicas", 0)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.multicast.ping_timeout", "5s")
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
    }

    @Override
    protected void waitForCluster() throws IOException {
        // do not wait for cluster health
    }

    @Test(expected = ClusterBlockException.class)
    public void testClusterBlockNodeClient() throws Exception {
            IngestRequestBuilder brb = client("1").prepareExecute(IngestAction.INSTANCE);
            XContentBuilder builder = jsonBuilder().startObject().field("field", "value").endObject();
            String jsonString = builder.string();
            IndexRequestBuilder irb = client("1").prepareExecute(IndexAction.INSTANCE)
                    .setIndex("test").setType("test").setId("1").setSource(jsonString);
            brb.add(irb);
            brb.execute().actionGet();
    }

    @Test(expected = MasterNotDiscoveredException.class)
    public void testClusterBlockTransportClient() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 3)
                .build();
            final IngestTransportClient ingest = new IngestTransportClient()
                    .newClient(getSettings())
                    .newIndex("test", settings, null);
            IngestRequestBuilder brb = ingest.client().prepareExecute(IngestAction.INSTANCE);
            XContentBuilder builder = jsonBuilder().startObject().field("field", "bvalue").endObject();
            String jsonString = builder.string();
            IndexRequestBuilder irb = ingest.client().prepareExecute(IndexAction.INSTANCE)
                    .setIndex("test").setType("test").setId("1").setSource(jsonString);
            brb.add(irb);
            brb.execute().actionGet();
    }

}

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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IngestClusterBlockTest extends AbstractNodeTestHelper {

    protected Settings getNodeSettings() {
        return ImmutableSettings
                .settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("cluster.routing.schedule", "50ms")
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("http.enabled", false)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.multicast.ping_timeout", "5s")
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
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
            final IngestTransportClient ingest = new IngestTransportClient()
                    .newClient(getSettings())
                    .shards(1)
                    .replica(0)
                    .newIndex("test");
            IngestRequestBuilder brb = ingest.client().prepareExecute(IngestAction.INSTANCE);
            XContentBuilder builder = jsonBuilder().startObject().field("field", "bvalue").endObject();
            String jsonString = builder.string();
            IndexRequestBuilder irb = ingest.client().prepareExecute(IndexAction.INSTANCE)
                    .setIndex("test").setType("test").setId("1").setSource(jsonString);
            brb.add(irb);
            brb.execute().actionGet();
    }


}

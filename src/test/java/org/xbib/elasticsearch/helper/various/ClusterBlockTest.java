package org.xbib.elasticsearch.helper.various;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.helper.helper.AbstractNodeTestHelper;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ClusterBlockTest extends AbstractNodeTestHelper {

    protected Settings getNodeSettings() {
        return ImmutableSettings
                .settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("cluster.routing.schedule", "50ms")
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("http.enabled", false)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
    }

    // HANGS!
    public void testClusterBlock() throws Exception {
        BulkRequestBuilder brb = client("1").prepareBulk();
        XContentBuilder builder = jsonBuilder().startObject().field("field1", "value1").endObject();
        String jsonString = builder.string();
        IndexRequestBuilder irb = client("1").prepareIndex("test", "test", "1").setSource(jsonString);
        brb.add(irb);
        BulkResponse bulkResponse = brb.execute().actionGet();
    }

}

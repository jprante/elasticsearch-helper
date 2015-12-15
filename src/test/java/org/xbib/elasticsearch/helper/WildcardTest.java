package org.xbib.elasticsearch.helper;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Test;
import org.xbib.elasticsearch.util.NodeTestUtils;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

public class WildcardTest extends NodeTestUtils {

    protected Settings getNodeSettings() {
        return settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("discovery.zen.multicast.enabled", false)
                .put("http.enabled", false)
                .put("path.home", System.getProperty("path.home"))
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
    }

    @Test
    public void testWildcard() throws Exception {
        index(client("1"), "1", "010");
        index(client("1"), "2", "0*0");
        // exact
        validateCount(client("1"), queryStringQuery("010").defaultField("field"), 1);
        validateCount(client("1"), queryStringQuery("0\\*0").defaultField("field"), 1);
        // pattern
        validateCount(client("1"), queryStringQuery("0*0").defaultField("field"), 1); // 2?
        validateCount(client("1"), queryStringQuery("0?0").defaultField("field"), 1); // 2?
        validateCount(client("1"), queryStringQuery("0**0").defaultField("field"), 1); // 2?
        validateCount(client("1"), queryStringQuery("0??0").defaultField("field"), 0);
        validateCount(client("1"), queryStringQuery("*10").defaultField("field"), 1);
        validateCount(client("1"), queryStringQuery("*1*").defaultField("field"), 1);
        validateCount(client("1"), queryStringQuery("*\\*0").defaultField("field"), 0); // 1?
        validateCount(client("1"), queryStringQuery("*\\**").defaultField("field"), 0); // 1?
    }

    private void index(Client client, String id, String fieldValue) throws IOException {
        client.index(indexRequest()
                .index("index").type("type").id(id)
                .source(jsonBuilder().startObject().field("field", fieldValue).endObject())
                .refresh(true)).actionGet();
    }

    private long count(Client client, QueryBuilder queryBuilder) {
        return client.prepareSearch("index").setTypes("type")
                .setQuery(queryBuilder)
                .execute().actionGet().getHits().getTotalHits();
    }

    private void validateCount(Client client, QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(client, queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }

}
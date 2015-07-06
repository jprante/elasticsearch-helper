package org.xbib.elasticsearch.support.various;

import java.io.IOException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.xbib.elasticsearch.support.client.ClientHelper;

import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class WildcardTest {

    @Test
    public void testWildcard() throws Exception {
        Node node = null;
        try {
            Settings settings = settingsBuilder()
                    .put("cluster.name", getClusterName())
                    .put("cluster.routing.allocation.disk.threshold_enabled", false)
                    .put("discovery.zen.multicast.enabled", false)
                    .put("gateway.type", "none")
                    .put("http.enabled", false)
                    .put("path.home", System.getProperty("path.home"))
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)

                    .put("index.analysis.analyzer.default.filter", "lowercase")
                    .put("index.analysis.analyzer.default.tokenizer", "keyword").build();
            node = nodeBuilder().settings(settings).local(true).node();
            node.start();
            Client client = node.client();
            ClientHelper.waitForCluster(client, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            index(client, "1", "010");
            index(client, "2", "0*0");
            // exact
            validateCount(client, queryStringQuery("010").defaultField("field"), 1);
            validateCount(client, queryStringQuery("0\\*0").defaultField("field"), 1);
            // pattern
            validateCount(client, queryStringQuery("0*0").defaultField("field"), 2);
            validateCount(client, queryStringQuery("0?0").defaultField("field"), 2);
            validateCount(client, queryStringQuery("0**0").defaultField("field"), 2);
            validateCount(client, queryStringQuery("0??0").defaultField("field"), 0);
            validateCount(client, queryStringQuery("*10").defaultField("field"), 1);
            validateCount(client, queryStringQuery("*1*").defaultField("field"), 1);
            validateCount(client, queryStringQuery("*\\*0").defaultField("field"), 1);
            validateCount(client, queryStringQuery("*\\**").defaultField("field"), 1);
            client.admin().indices().delete(deleteIndexRequest("index")).actionGet();
        } finally {
            if (node !=null) {
                node.close();
            }
        }
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

    private String getClusterName() {
        return "test-support-cluster-" + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name");
    }

}
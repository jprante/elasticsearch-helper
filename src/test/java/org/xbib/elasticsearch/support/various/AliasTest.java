
package org.xbib.elasticsearch.support.various;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.xbib.elasticsearch.support.client.ClientHelper;

import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertTrue;

public class AliasTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(AliasTest.class.getName());

    @Test
    public void testAlias() throws IOException {
        Node node = null;
        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", getClusterName())
                    .put("cluster.routing.allocation.disk.threshold_enabled", false)
                    .put("discovery.zen.multicast.enabled", false)
                    .put("gateway.type", "none")
                    .put("http.enabled", false)
                    .put("path.home", System.getProperty("path.home"))
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            node = nodeBuilder().settings(settings).local(true).node();
            node.start();
            Client client = node.client();
            ClientHelper.waitForCluster(client, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));

            // create index
            CreateIndexRequest indexRequest = new CreateIndexRequest("test");
            client.admin().indices().create(indexRequest).actionGet();

            // put alias
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            String[] indices = new String[]{"test"};
            String[] aliases = new String[]{"test_alias"};
            IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(AliasAction.Type.ADD, indices, aliases);
            indicesAliasesRequest.addAliasAction(aliasAction);
            client.admin().indices().aliases(indicesAliasesRequest).actionGet();

            // get alias
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest(Strings.EMPTY_ARRAY);
            long t0 = System.nanoTime();
            GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(getAliasesRequest).actionGet();
            long t1 = (System.nanoTime() - t0) / 1000000;
            logger.info("{} time(ms) = {}", getAliasesResponse.getAliases(), t1);
            assertTrue(t1 > 0);
        } finally {
            if (node !=null){
                node.close();
            }
        }
    }

    private String getClusterName() {
        return "test-support-cluster-" + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name");
    }

}

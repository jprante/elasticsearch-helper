
package org.xbib.elasticsearch.support;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.Node;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.*;

public class QueryTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(QueryTest.class.getName());

    public void testQuery() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "test").build();

        String index = "test";
        String type = "test";
        Node node = null;
        try {
            node = nodeBuilder().settings(settings).client(true).node();
            Client client = node.client();
            SearchResponse response = client.prepareSearch().setIndices(index).
                    setTypes(type).
                    setFrom(0).setSize(10).setQuery(matchQuery("_all", "test")).execute().actionGet();
        } catch (Exception e) {
            logger.warn(e.getMessage());
        } finally {
            if (node !=null){
                node.stop();
                node.close();
            }
        }
    }

    public void testSniff() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "test")
                .put("client.transport.sniff", false).build();
        try {
            TransportClient client = new TransportClient(settings);
            InetSocketTransportAddress address = new InetSocketTransportAddress("localhost", 9300);
            client.addTransportAddress(address);
            client.close();
        } catch (MasterNotDiscoveredException e) {
            logger.warn(e.getMessage());
        }
    }
}

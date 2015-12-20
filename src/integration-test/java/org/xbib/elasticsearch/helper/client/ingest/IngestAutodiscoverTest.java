package org.xbib.elasticsearch.helper.client.ingest;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Test;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.IngestTransportClient;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.NodeTestUtils;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class IngestAutodiscoverTest extends NodeTestUtils {

    @Test
    public void testAutodiscover() throws IOException {
        startNode("2");
        Settings.Builder settingsBuilder = Settings.builder()
                .put("cluster.name", getClusterName())
                .put("path.home", System.getProperty("path.home"))
                .put("autodiscover", true);
        int i = 0;
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        for (NodeInfo nodeInfo : response) {
            TransportAddress ta = nodeInfo.getTransport().getAddress().publishAddress();
            if (ta instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress address = (InetSocketTransportAddress) ta;
                settingsBuilder.put("host." + i++, address.address().getHostName() + ":" + address.address().getPort());
            }
        }
        final IngestTransportClient ingest = ClientBuilder.builder()
                .put(settingsBuilder.build())
                .setMetric(new LongAdderIngestMetric())
                .toIngestTransportClient();
        try {
            ingest.newIndex("test");
        } finally {
            ingest.shutdown();
        }
        if (ingest.hasThrowable()) {
            logger.error("error", ingest.getThrowable());
        }
        assertFalse(ingest.hasThrowable());
    }

}

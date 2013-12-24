
package org.xbib.elasticsearch.support.client;

import java.net.URI;

import org.elasticsearch.client.Client;

import org.xbib.elasticsearch.action.search.support.BasicRequest;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Search client support
 */
public class SearchClient extends TransportClientBase implements Search {

    private String index;

    private String type;

    public SearchClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public SearchClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public SearchClient newClient() {
        this.newClient(findURI());
        return this;
    }

    @Override
    public SearchClient newClient(URI uri) {
        this.newClient(uri, settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false)
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s")
                .build());
        return this;
    }

    public Client client() {
        return super.client();
    }

    @Override
    public BasicRequest newSearchRequest() {
        return new BasicRequest()
                .newSearchRequest(client.prepareSearch().setPreference("_primary_first"));
    }

    @Override
    public BasicRequest newGetRequest() {
        return new BasicRequest()
                .newGetRequest(client.prepareGet());
    }

}

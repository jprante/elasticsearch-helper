
package org.xbib.elasticsearch.support.client;

import java.net.URI;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import org.xbib.elasticsearch.action.search.support.BasicRequest;

/**
 * Search client support
 */
public class SearchClient extends TransportClientBase implements Search {

    protected Settings settings;

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
        this.newClient(findURI(), settings);
        return this;
    }

    @Override
    public SearchClient newClient(URI uri) {
        this.newClient(findURI(), ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true) // node client
                .put("client.transport.sniff", false) // sniff would join us into any cluster ...?
                .build());
        return this;
    }

    public SearchClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
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

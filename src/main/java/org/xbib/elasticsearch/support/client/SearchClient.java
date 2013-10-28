
package org.xbib.elasticsearch.support.client;

import java.net.URI;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import org.xbib.elasticsearch.action.search.support.BasicRequest;

/**
 * Search client support
 */
public class SearchClient extends AbstractClient implements Search {

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
        super.newClient();
        return this;
    }

    @Override
    public SearchClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    public Client client() {
        return super.client();
    }

    /**
     * Create settings
     *
     * @param uri
     * @param n the client thread pool size
     * @return the settings
     */
    protected Settings initialSettings(URI uri, int n) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .put("network.server", false)
                .put("node.client", true) // node client
                .put("client.transport.sniff", false) // sniff would join us into any cluster ...?
                .build();
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

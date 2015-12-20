package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.action.search.helper.BasicGetRequest;
import org.xbib.elasticsearch.action.search.helper.BasicSearchRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Search client support
 */
public class SearchTransportClient extends BaseTransportClient implements Search {

    private String index;

    private String type;

    public String getIndex() {
        return index;
    }

    public SearchTransportClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public SearchTransportClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public SearchTransportClient init(Settings settings) throws IOException {
        super.createClient(settings);
        return this;
    }

    @Override
    public SearchTransportClient init(Map<String, String> settings) throws IOException {
        super.createClient(settings);
        return this;
    }

    public Client client() {
        return client;
    }

    @Override
    public BasicSearchRequest newSearchRequest() {
        return new BasicSearchRequest()
                .newRequest(client.prepareSearch());
    }

    @Override
    public BasicGetRequest newGetRequest() {
        return new BasicGetRequest()
                .newRequest(client.prepareGet());
    }

    public String clusterName() {
        return ClientHelper.clusterName(client);
    }

    public String healthColor() {
        return ClientHelper.healthColor(client);
    }

}

package org.xbib.elasticsearch.support.client.search;

import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.action.search.support.BasicGetRequest;
import org.xbib.elasticsearch.action.search.support.BasicSearchRequest;
import org.xbib.elasticsearch.support.client.BaseTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Search;

import java.io.IOException;
import java.util.Map;

/**
 * Search client support
 */
public class SearchClient extends BaseTransportClient implements Search {

    private String index;

    private String type;

    public String getIndex() {
        return index;
    }

    public SearchClient setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public SearchClient setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public SearchClient init(Settings settings) throws IOException {
        super.createClient(settings);
        return this;
    }

    @Override
    public SearchClient init(Map<String, String> settings) throws IOException {
        super.createClient(settings);
        return this;
    }

    public AbstractClient client() {
        return super.client();
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

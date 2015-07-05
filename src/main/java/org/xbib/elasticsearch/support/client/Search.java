package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.action.search.support.BasicGetRequest;
import org.xbib.elasticsearch.action.search.support.BasicSearchRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Search support
 */
public interface Search {

    Search init(Settings settings) throws IOException;

    Search init(Map<String,String> settings) throws IOException;

    /**
     * Return the Elasticsearch client
     *
     * @return the client
     */
    ElasticsearchClient client();

    /**
     * Set index
     *
     * @param index index
     * @return this search
     */
    Search setIndex(String index);

    /**
     * Get index
     *
     * @return the index
     */
    String getIndex();

    /**
     * Create new search request
     * @return this search request
     */
    BasicSearchRequest newSearchRequest();

    /**
     * Create new get request
     * @return this search request
     */
    BasicGetRequest newGetRequest();

    /**
     * Shutdown and release all resources
     */
    void shutdown();

}

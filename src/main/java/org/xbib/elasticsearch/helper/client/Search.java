package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.client.Client;
import org.xbib.elasticsearch.action.search.support.BasicGetRequest;
import org.xbib.elasticsearch.action.search.support.BasicSearchRequest;

/**
 * Search support
 */
public interface Search {

    /**
     * Return the Elasticsearch client
     *
     * @return the client
     */
    Client client();

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
     */
    BasicSearchRequest newSearchRequest();

    /**
     * Create new get request
     */
    BasicGetRequest newGetRequest();

    /**
     * Shutdown and release all resources
     */
    void shutdown();

}

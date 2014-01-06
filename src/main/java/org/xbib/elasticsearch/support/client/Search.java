
package org.xbib.elasticsearch.support.client;

import org.xbib.elasticsearch.action.search.support.BasicRequest;
import org.elasticsearch.client.Client;

import java.net.URI;

/**
 * Search support
 */
public interface Search {

    /**
     * Return the Elasticsearch client
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
    BasicRequest newSearchRequest();

    /**
     * Create new get request
     */
    BasicRequest newGetRequest();

    /**
     * Shutdown and release all resources
     */
    void shutdown();

}

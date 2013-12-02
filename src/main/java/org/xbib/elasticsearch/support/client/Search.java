
package org.xbib.elasticsearch.support.client;

import org.xbib.elasticsearch.action.search.support.BasicRequest;
import org.elasticsearch.client.Client;

import java.net.URI;

/**
 * Transport client search
 */
public interface Search {

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

    String getType();

    /**
     * Create a new client
     */
    Search newClient();

    /**
     * Create a new client
     */
    Search newClient(URI uri);

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

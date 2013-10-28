
package org.xbib.elasticsearch.support.client;

import org.xbib.elasticsearch.action.search.support.BasicRequest;
import org.elasticsearch.client.Client;

import java.net.URI;

/**
 * Transport client search helper API
 */
public interface Search {

    Client client();

    /**
     * Set index
     *
     * @param index
     * @return this TransportClientHelper
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
     * Shutdown, free all resources
     */
    void shutdown();

}

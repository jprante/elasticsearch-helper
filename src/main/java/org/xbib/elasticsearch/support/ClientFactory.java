package org.xbib.elasticsearch.support;

import java.net.URI;

public interface ClientFactory {

    /**
     * Create a new client
     *
     * @return this TransportClientIndexer
     */
    ClientIngester newClient();

    /**
     * Create a new client
     *
     * @param uri the URI to connect to
     * @return this TransportClientIndexer
     */
    ClientIngester newClient(URI uri);

}

package org.xbib.elasticsearch.support.client;

import java.net.URI;

public interface ClientFactory {

    /**
     * Create a new client
     *
     * @return this ingest
     */
    Ingest newClient();

    /**
     * Create a new client
     *
     * @param uri the URI to connect to
     * @return this ingest
     */
    Ingest newClient(URI uri);

}

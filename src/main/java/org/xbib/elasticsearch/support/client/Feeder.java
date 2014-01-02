
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.Client;

/**
 * Minimal API for feed
 */
public interface Feeder {

    Client client();

    /**
     * Create document
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @param source the source
     * @return this document ingest
     */
    Feeder create(String index, String type, String id, String source);

    /**
     * Index document
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @param source the source
     * @return this document ingest
     */
    Feeder index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index the index
     * @param type the type
     * @param id the id
     * @return this document ingest
     */
    Feeder delete(String index, String type, String id);


}

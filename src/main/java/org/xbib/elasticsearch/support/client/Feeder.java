package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.Client;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;

/**
 * Minimal API for feed
 */
public interface Feeder {

    Client client();

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Feeder index(String index, String type, String id, String source);

    /**
     * Index document
     *
     * @param indexRequest the index request
     * @return this
     */
    Feeder index(IndexRequest indexRequest);

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this
     */
    Feeder delete(String index, String type, String id);

    Feeder delete(DeleteRequest deleteRequest);


}

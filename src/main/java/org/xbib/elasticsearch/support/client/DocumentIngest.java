
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.Client;

/**
 * Minimal API for document ingesting
 */
public interface DocumentIngest {

    Client client();

    /**
     * Set the default index
     *
     * @param index the index
     * @return this document ingest
     */
    DocumentIngest setIndex(String index);

    /**
     * Returns the default index
     *
     * @return the index
     */
    String getIndex();

    /**
     * Set the default type
     *
     * @param type the type
     * @return this document ingest
     */
    DocumentIngest setType(String type);

    /**
     * Returns the default type
     *
     * @return the type
     */
    String getType();

    /**
     * Create document
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return this document ingest
     */
    DocumentIngest createDocument(String index, String type, String id, String source);

    /**
     * Index document
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return this document ingest
     */
    DocumentIngest indexDocument(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index
     * @param type
     * @param id
     * @return this document ingest
     */
    DocumentIngest deleteDocument(String index, String type, String id);

    /**
     * Ensure that all documents arrive.
     *
     * @return this document ingest
     */
    DocumentIngest flush();

    /**
     *
     * Shutdown this client
     */
    void shutdown();
}

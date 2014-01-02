
package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.client.Client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Mock ingest client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockIngestClient extends IngestClient {

    @Override
    public MockIngestClient newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockIngestClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public String getIndex() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public MockIngestClient dateDetection(boolean dateDetection) {
        return this;
    }

    @Override
    public MockIngestClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockIngestClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockIngestClient waitForCluster() throws IOException {
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return -1;
    }

    @Override
    public MockIngestClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public MockIngestClient setType(String type) {
        super.setType(type);
        return this;
    }

    @Override
    public MockIngestClient setting(String key, String value) {
        return this;
    }

    @Override
    public MockIngestClient setting(String key, Integer value) {
        return this;
    }

    @Override
    public MockIngestClient setting(String key, Boolean value) {
        return this;
    }

    @Override
    public MockIngestClient setting(InputStream in) throws IOException{
        return this;
    }

    @Override
    public MockIngestClient mapping(String type, InputStream in) throws IOException {
        return this;
    }

    @Override
    public MockIngestClient mapping(String type, String mapping) {
        return this;
    }

    @Override
    public MockIngestClient create(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockIngestClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockIngestClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockIngestClient flush() {
        return this;
    }

    @Override
    public MockIngestClient startBulk() {
        return this;
    }

    @Override
    public MockIngestClient stopBulk() {
        return this;
    }

    @Override
    public MockIngestClient deleteIndex() {
        return this;
    }

    @Override
    public MockIngestClient newIndex() {
        return this;
    }

    @Override
    public MockIngestClient putMapping(String index) {
        return this;
    }

    @Override
    public MockIngestClient refresh() {
        return this;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}

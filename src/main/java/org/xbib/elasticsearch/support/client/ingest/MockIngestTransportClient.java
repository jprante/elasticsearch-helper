
package org.xbib.elasticsearch.support.client.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Mock ingest client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockIngestTransportClient extends IngestTransportClient {

    @Override
    public MockIngestTransportClient newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockIngestTransportClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public MockIngestTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
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
    public MockIngestTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockIngestTransportClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockIngestTransportClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public MockIngestTransportClient setType(String type) {
        super.setType(type);
        return this;
    }

    @Override
    public MockIngestTransportClient setting(String key, String value) {
        return this;
    }

    @Override
    public MockIngestTransportClient setting(String key, Integer value) {
        return this;
    }

    @Override
    public MockIngestTransportClient setting(String key, Boolean value) {
        return this;
    }

    @Override
    public MockIngestTransportClient setting(InputStream in) throws IOException{
        return this;
    }

    @Override
    public MockIngestTransportClient mapping(String type, InputStream in) throws IOException {
        return this;
    }

    @Override
    public MockIngestTransportClient mapping(String type, String mapping) {
        return this;
    }

    @Override
    public MockIngestTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockIngestTransportClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockIngestTransportClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockIngestTransportClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockIngestTransportClient flush() {
        return this;
    }

    @Override
    public MockIngestTransportClient startBulk() {
        return this;
    }

    @Override
    public MockIngestTransportClient stopBulk() {
        return this;
    }

    @Override
    public MockIngestTransportClient deleteIndex() {
        return this;
    }

    @Override
    public MockIngestTransportClient newIndex() {
        return this;
    }

    @Override
    public MockIngestTransportClient putMapping(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient refresh() {
        return this;
    }

    @Override
    public MockIngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        return this;
    }

    @Override
    public int waitForRecovery() throws IOException {
        return -1;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return -1;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}

package org.xbib.elasticsearch.support.client.ingest;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.net.URI;

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
    public MockIngestTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockIngestTransportClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
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
    public MockIngestTransportClient flushIngest() {
        return this;
    }

    @Override
    public MockIngestTransportClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockIngestTransportClient startBulk(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient newIndex(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient putMapping(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient refresh(String index) {
        return this;
    }

    @Override
    public MockIngestTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return -1;
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return -1;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}

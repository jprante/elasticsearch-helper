package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.action.delete.DeleteRequest;
import org.xbib.elasticsearch.action.index.IndexRequest;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;
import java.net.URI;

/**
 * Mock client for Bulk API. Do not perform actions on a real cluster.
 * Useful for testing or dry runs.
 */
public class MockBulkTransportClient extends BulkTransportClient implements Ingest {

    @Override
    public MockBulkTransportClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockBulkTransportClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockBulkTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public MockBulkTransportClient newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockBulkTransportClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public MockBulkTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        return this;
    }

    @Override
    public MockBulkTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockBulkTransportClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockBulkTransportClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockBulkTransportClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockBulkTransportClient flushIngest() {
        return this;
    }

    @Override
    public MockBulkTransportClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockBulkTransportClient startBulk(String index) throws IOException {
        return this;
    }

    @Override
    public MockBulkTransportClient stopBulk(String index) throws IOException {
        return this;
    }

    @Override
    public MockBulkTransportClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockBulkTransportClient newIndex(String index) {
        return this;
    }

    @Override
    public MockBulkTransportClient putMapping(String index) {
        return this;
    }

    @Override
    public MockBulkTransportClient refresh(String index) {
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
    }

}

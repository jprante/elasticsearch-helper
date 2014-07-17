package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;
import java.net.URI;

/**
 * Mock client for Bulk API. Do not perform actions on a real cluster.
 * Useful for testing or dry runs.
 */
public class MockNodeClient extends NodeClient implements Ingest {

    @Override
    public MockNodeClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockNodeClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockNodeClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public MockNodeClient newClient(Client client) {
        super.newClient(client);
        return this;
    }

    @Override
    public MockNodeClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public MockNodeClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockNodeClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockNodeClient bulkIndex(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockNodeClient bulkDelete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockNodeClient flushIngest() {
        return this;
    }

    @Override
    public MockNodeClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockNodeClient startBulk(String index) throws IOException {
        return this;
    }

    @Override
    public MockNodeClient stopBulk(String index) throws IOException {
        return this;
    }

    @Override
    public MockNodeClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockNodeClient newIndex(String index) {
        return this;
    }

    @Override
    public MockNodeClient putMapping(String index) {
        return this;
    }

    @Override
    public MockNodeClient refresh(String index) {
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

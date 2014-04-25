
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;
import java.io.InputStream;
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
    public MockBulkTransportClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public MockBulkTransportClient setType(String type) {
        super.setType(type);
        return this;
    }

    @Override
    public MockBulkTransportClient setting(String key, String value) {
        return this;
    }

    @Override
    public MockBulkTransportClient setting(String key, Integer value) {
        return this;
    }

    @Override
    public MockBulkTransportClient setting(String key, Boolean value) {
        return this;
    }

    @Override
    public MockBulkTransportClient setting(InputStream in) throws IOException{
        return this;
    }

    @Override
    public MockBulkTransportClient mapping(String type, InputStream in) throws IOException {
        return this;
    }

    @Override
    public MockBulkTransportClient mapping(String type, String mapping) {
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
    public MockBulkTransportClient flush() {
        return this;
    }

    @Override
    public MockBulkTransportClient startBulk() throws IOException {
        return this;
    }

    @Override
    public MockBulkTransportClient stopBulk() throws IOException {
        return this;
    }

    @Override
    public MockBulkTransportClient deleteIndex() {
        return this;
    }

    @Override
    public MockBulkTransportClient newIndex() {
        return this;
    }

    @Override
    public MockBulkTransportClient putMapping(String index) {
        return this;
    }

    @Override
    public MockBulkTransportClient refresh() {
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
    }

}

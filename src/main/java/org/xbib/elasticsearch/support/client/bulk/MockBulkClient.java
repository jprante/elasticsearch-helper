
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
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
public class MockBulkClient extends BulkClient implements Ingest {

    @Override
    public MockBulkClient maxActionsPerBulkRequest(int maxBulkActions) {
        return this;
    }

    @Override
    public MockBulkClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockBulkClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public MockBulkClient newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockBulkClient newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    @Override
    public MockBulkClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        return this;
    }

    @Override
    public MockBulkClient dateDetection(boolean dateDetection) {
        return this;
    }

    @Override
    public void waitForCluster() throws IOException {
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return -1;
    }

    @Override
    public long getTotalBulkRequests() {
        return 0;
    }

    @Override
    public long getTotalBulkRequestTime() {
        return 0;
    }

    @Override
    public long getTotalSizeInBytes() {
        return 0;
    }

    @Override
    public MockBulkClient setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public MockBulkClient setType(String type) {
        super.setType(type);
        return this;
    }

    @Override
    public MockBulkClient setting(String key, String value) {
        return this;
    }

    @Override
    public MockBulkClient setting(String key, Integer value) {
        return this;
    }

    @Override
    public MockBulkClient setting(String key, Boolean value) {
        return this;
    }

    @Override
    public MockBulkClient setting(InputStream in) throws IOException{
        return this;
    }

    @Override
    public MockBulkClient mapping(String type, InputStream in) throws IOException {
        return this;
    }

    @Override
    public MockBulkClient mapping(String type, String mapping) {
        return this;
    }

    @Override
    public MockBulkClient index(String index, String type, String id, BytesReference source) {
        return this;
    }

    @Override
    public MockBulkClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockBulkClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockBulkClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockBulkClient flush() {
        return this;
    }

    @Override
    public MockBulkClient startBulk() throws IOException {
        return this;
    }

    @Override
    public MockBulkClient stopBulk() throws IOException {
        return this;
    }

    @Override
    public MockBulkClient deleteIndex() {
        return this;
    }

    @Override
    public MockBulkClient newIndex() {
        return this;
    }

    @Override
    public MockBulkClient putMapping(String index) {
        return this;
    }

    @Override
    public MockBulkClient refresh() {
        return this;
    }

    @Override
    public void shutdown() {
    }

}

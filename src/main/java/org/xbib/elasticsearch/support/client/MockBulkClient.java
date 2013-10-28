
package org.xbib.elasticsearch.support.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

/**
 * Mock client for Bulk API. Do not perform actions on a real cluster.
 * Useful for testing or dry runs.
 *
 */
public class MockBulkClient extends BulkClient implements Ingest {

    public Client client() {
        return null;
    }

    /**
     * No special initial settings except cluster name
     *
     * @param uri
     * @return initial settings
     */
    @Override
    protected Settings initialSettings(URI uri, int n) {
        return ImmutableSettings.settingsBuilder()
                .put("cluster.name", findClusterName(uri))
                .build();
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
    public MockBulkClient dateDetection(boolean dateDetection) {
        return this;
    }

    @Override
    public MockBulkClient maxBulkActions(int maxBulkActions) {
        return this;
    }

    @Override
    public MockBulkClient maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockBulkClient waitForCluster() throws IOException {
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return -1;
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
    public MockBulkClient createDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockBulkClient indexDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockBulkClient deleteDocument(String index, String type, String id) {
        return this;
    }

    @Override
    public MockBulkClient flush() {
        return this;
    }

    @Override
    public MockBulkClient startBulkMode() {
        return this;
    }

    @Override
    public MockBulkClient stopBulkMode() {
        return this;
    }

    @Override
    public long getVolumeInBytes() {
        return 0L;
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
    public MockBulkClient newType() {
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

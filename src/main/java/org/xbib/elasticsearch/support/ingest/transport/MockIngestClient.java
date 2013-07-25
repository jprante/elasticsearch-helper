/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.support.ingest.transport;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

/**
 * Mock ingest client. Do not perform actions on a real cluster.
 * Useful for testing or dry runs.
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class MockIngestClient extends IngestClient {

    private final static ESLogger logger = Loggers.getLogger(MockIngestClient.class);

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
    public MockIngestClient maxBulkActions(int maxBulkActions) {
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
    public MockIngestClient createDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockIngestClient indexDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockIngestClient deleteDocument(String index, String type, String id) {
        return this;
    }

    @Override
    public MockIngestClient flush() {
        return this;
    }

    @Override
    public MockIngestClient startBulkMode() {
        return this;
    }

    @Override
    public MockIngestClient stopBulkMode() {
        return this;
    }

    @Override
    public long getVolumeInBytes() {
        return 0L;
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
    public MockIngestClient newType() {
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

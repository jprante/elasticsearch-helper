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
import org.xbib.elasticsearch.support.TransportClientIngest;

import java.io.IOException;
import java.net.URI;

/**
 * TransportClientIngest Mockup. Do not perform actions on a real cluster.
 * Useful for testing or dry runs.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class MockTransportClientIngest extends TransportClientIngestSupport implements TransportClientIngest {

    private final static ESLogger logger = Loggers.getLogger(MockTransportClientIngest.class);

    @Override
    public MockTransportClientIngest newClient() {
        super.newClient();
        return this;
    }

    @Override
    public MockTransportClientIngest newClient(URI uri) {
        super.newClient(uri);
        return this;
    }

    public Client client() {
        return null;
    }

    @Override
    public MockTransportClientIngest dateDetection(boolean dateDetection) {
        return this;
    }

    @Override
    public MockTransportClientIngest maxBulkActions(int maxBulkActions) {
        return this;
    }

    @Override
    public MockTransportClientIngest maxConcurrentBulkRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockTransportClientIngest waitForHealthyCluster() throws IOException {
        return this;
    }

    @Override
    public int updateReplicaLevel(int level) throws IOException {
        return -1;
    }

    @Override
    public MockTransportClientIngest setIndex(String index) {
        super.setIndex(index);
        return this;
    }

    @Override
    public MockTransportClientIngest setType(String type) {
        super.setType(type);
        return this;
    }

    @Override
    public MockTransportClientIngest createDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClientIngest indexDocument(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClientIngest deleteDocument(String index, String type, String id) {
        return this;
    }

    @Override
    public MockTransportClientIngest flush() {
        return this;
    }

    @Override
    public MockTransportClientIngest startBulkMode() {
        return this;
    }

    @Override
    public MockTransportClientIngest stopBulkMode() {
        return this;
    }

    @Override
    public long getVolumeInBytes() {
        return 0L;
    }

    @Override
    public MockTransportClientIngest deleteIndex() {
        return this;
    }

    @Override
    public MockTransportClientIngest newIndex() {
        logger.info("mock newIndex");
        return this;
    }
}

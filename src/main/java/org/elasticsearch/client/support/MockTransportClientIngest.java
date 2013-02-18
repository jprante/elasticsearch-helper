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
package org.elasticsearch.client.support;

import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

/**
 * TransportClientIngest Mockup. Do not perform actions on a real cluster.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class MockTransportClientIngest extends MockTransportClientSearch implements TransportClientIngest {

    private String type;

    @Override
    public MockTransportClientIngest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

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
    public MockTransportClientIngest index(String index) {
        super.index(index);
        return this;
    }

    @Override
    public MockTransportClientIngest type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public MockTransportClientIngest create(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClientIngest index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClientIngest delete(String index, String type, String id) {
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
        return this;
    }
}

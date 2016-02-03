/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Map;

/**
 * Mock client, it does not perform actions on a cluster.
 * Useful for testing or dry runs.
 */
public class MockTransportClient extends BulkTransportClient {

    MockTransportClient() {
    }

    @Override
    public ElasticsearchClient client() {
        return null;
    }

    @Override
    public MockTransportClient init(ElasticsearchClient client, IngestMetric metric) {
        return this;
    }

    @Override
    public MockTransportClient init(Settings settings, IngestMetric metric) {
        return this;
    }

    @Override
    public MockTransportClient maxActionsPerRequest(int maxActions) {
        return this;
    }

    @Override
    public MockTransportClient maxConcurrentRequests(int maxConcurrentRequests) {
        return this;
    }

    @Override
    public MockTransportClient maxVolumePerRequest(ByteSizeValue maxVolumePerRequest) {
        return this;
    }

    @Override
    public MockTransportClient flushIngestInterval(TimeValue interval) {
        return this;
    }

    @Override
    public MockTransportClient index(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClient delete(String index, String type, String id) {
        return this;
    }

    @Override
    public MockTransportClient update(String index, String type, String id, String source) {
        return this;
    }

    @Override
    public MockTransportClient bulkIndex(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockTransportClient bulkDelete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockTransportClient bulkUpdate(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public MockTransportClient flushIngest() {
        return this;
    }

    @Override
    public MockTransportClient waitForResponses(TimeValue timeValue) throws InterruptedException {
        return this;
    }

    @Override
    public MockTransportClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockTransportClient stopBulk(String index) {
        return this;
    }

    @Override
    public MockTransportClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient newIndex(String index) {
        return this;
    }

    @Override
    public MockTransportClient newMapping(String index, String type, Map<String, Object> mapping) {
        return this;
    }

    @Override
    public void putMapping(String index) {
    }

    @Override
    public void refreshIndex(String index) {
    }

    @Override
    public void flushIndex(String index) {
    }

    @Override
    public void waitForCluster(String healthColor, TimeValue timeValue) throws IOException {
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

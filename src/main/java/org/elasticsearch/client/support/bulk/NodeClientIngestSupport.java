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
package org.elasticsearch.client.support.bulk;

import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClientIngest support. Implements minimal API for node client ingesting.
 * Useful for river implementations.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class NodeClientIngestSupport implements ClientIngest {

    private final static ESLogger logger = Loggers.getLogger(NodeClientIngestSupport.class);

    private Client client;

    private String index;

    private String type;

    /**
     * The default size of a request
     */
    private int maxBulkActions = 100;
    /**
     * The default number of maximum concurrent ingestProcessor requests
     */
    private int maxConcurrentBulkRequests = 30;
    /**
     * The outstanding requests
     */
    private final AtomicLong outstandingBulkRequests = new AtomicLong();
    /**
     * Count the volume
     */
    private final AtomicLong volumeCounter = new AtomicLong();
    /**
     * Enabled of not
     */
    private boolean enabled = true;
    /**
     * The bulk processor
     */
    private final BulkProcessor bulkProcessor;

    public NodeClientIngestSupport(Client client, String index, String type,
                                   int maxBulkActions, int maxConcurrentBulkRequests) {
        this.client = client;
        this.index = index;
        this.type = type;
        this.maxBulkActions = maxBulkActions;
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = outstandingBulkRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                logger.info("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.info("bulk [{}] success [{} items] [{}ms]",
                        executionId, response.getItems().length, response.getTook().millis());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.error("bulk ["+executionId+"] error", failure);
                enabled = false;
            }
        };
        this.bulkProcessor = BulkProcessor.builder(client, listener)
                .setBulkActions(maxBulkActions-1) // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .build();
        try {
            waitForHealthyCluster();
            this.enabled = true;
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            this.enabled = false;
        }
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    @Override
    public NodeClientIngestSupport createDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id).create(true).source(source);
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("bulk add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public NodeClientIngestSupport indexDocument(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id).create(false).source(source);
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("bulk add of index failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public NodeClientIngestSupport deleteDocument(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index != null ? index : getIndex())
                .type(type != null ? type : getType()).id(id);
        try {
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public NodeClientIngestSupport flush() {
        if (!enabled) {
            return this;
        }
        // no nothing
        return this;
    }

    public NodeClientIngestSupport waitForHealthyCluster() throws IOException {
        return waitForHealthyCluster(ClusterHealthStatus.YELLOW, "30s");
    }

    public NodeClientIngestSupport waitForHealthyCluster(ClusterHealthStatus status, String timeout) throws IOException {
        try {
            logger.info("waiting for cluster health...");
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster not healthy, cowardly refusing to continue with operations");
            }
        } catch (ElasticSearchTimeoutException e) {
            throw new IOException("cluster not healthy, cowardly refusing to continue with operations");
        }
        return this;
    }
}

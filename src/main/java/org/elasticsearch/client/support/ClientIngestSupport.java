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

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.IngestProcessor;
import org.elasticsearch.action.bulk.IngestRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ClientIngest support. Implements minimal API for node client ingesting.
 * Useful for river implementations.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class ClientIngestSupport implements ClientIngest {

    private final static ESLogger logger = Loggers.getLogger(ClientIngestSupport.class);

    private String index;

    private String type;

    /**
     * The default size of a ingestProcessor request
     */
    private int maxBulkActions = 100;
    /**
     * The default number of maximum concurrent ingestProcessor requests
     */
    private int maxConcurrentBulkRequests = 30;
    /**
     * The outstanding ingestProcessor requests
     */
    private final AtomicLong outstandingBulkRequests = new AtomicLong();
    /**
     * Count the ingestProcessor volume
     */
    private final AtomicLong volumeCounter = new AtomicLong();
    private boolean enabled = true;
    private final IngestProcessor ingestProcessor;

    public ClientIngestSupport(Client client, String index, String type,
                               int maxBulkActions, int maxConcurrentBulkRequests) {
        this.index = index;
        this.type =type;
        this.maxBulkActions = maxBulkActions;
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, IngestRequest request) {
                long l = outstandingBulkRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(request.estimatedSizeInBytes());
                logger.info("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, request.numberOfActions(), v, l);
            }

            @Override
            public void afterBulk(long executionId, BulkResponse response) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.info("bulk [{}] success [{} items] [{}ms]",
                        executionId, response.items().length, response.took().millis());
            }

            @Override
            public void afterBulk(long executionId, Throwable failure) {
                long l = outstandingBulkRequests.decrementAndGet();
                logger.error("bulk [{}] error", executionId, failure);
                enabled = false;
            }
        };
        this.ingestProcessor = IngestProcessor.builder(client)
                .listener(listener)
                .actions(maxBulkActions)
                .concurrency(maxConcurrentBulkRequests)
                .build();
        this.enabled = true;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    @Override
    public ClientIngestSupport create(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(true).source(source);
        try {
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("bulk add of create failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public ClientIngestSupport index(String index, String type, String id, String source) {
        if (!enabled) {
            return this;
        }
        IndexRequest indexRequest = Requests.indexRequest(index).type(type).id(id).create(false).source(source);
        try {
            ingestProcessor.add(indexRequest);
        } catch (Exception e) {
            logger.error("bulk add of index failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public ClientIngestSupport delete(String index, String type, String id) {
        if (!enabled) {
            return this;
        }
        DeleteRequest deleteRequest = Requests.deleteRequest(index).type(type).id(id);
        try {
            ingestProcessor.add(deleteRequest);
        } catch (Exception e) {
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
            enabled = false;
        }
        return this;
    }

    @Override
    public ClientIngestSupport flush() {
        if (!enabled) {
            return this;
        }
        ingestProcessor.flush();
        return this;
    }
}

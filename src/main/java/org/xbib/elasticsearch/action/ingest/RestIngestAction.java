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
package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 */

public class RestIngestAction extends BaseRestHandler {

    private final static ESLogger logger = Loggers.getLogger(RestIngestAction.class);
    /**
     * The outstanding requests
     */
    public final static AtomicLong outstandingRequests = new AtomicLong();
    /**
     * Count the volume
     */
    public final static AtomicLong volumeCounter = new AtomicLong();
    /**
     * The responses
     */
    public final static Map<Long,Object> responses = new LRUHashMap(1000);
    /**
     * The IngestProcessor
     */
    private final IngestProcessor ingestProcessor;

    @Inject
    public RestIngestAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(POST, "/_ingest", this);
        controller.registerHandler(PUT, "/_ingest", this);
        controller.registerHandler(POST, "/{index}/_ingest", this);
        controller.registerHandler(PUT, "/{index}/_ingest", this);
        controller.registerHandler(POST, "/{index}/{type}/_ingest", this);
        controller.registerHandler(PUT, "/{index}/{type}/_ingest", this);

        int actions = settings.getAsInt("action.ingest.maxactions", 100);
        int concurrency = settings.getAsInt("action.ingest.maxconcurrency", 30);
        ByteSizeValue volume = settings.getAsBytesSize("action.ingest.maxvolume", ByteSizeValue.parseBytesSizeValue("5m"));

        this.ingestProcessor = IngestProcessor.builder(client)
                .actions(actions)
                .concurrency(concurrency)
                .maxVolume(volume)
                .build();
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        final ExecutionIdHolder idHolder = new ExecutionIdHolder();
        final CountDownLatch latch = new CountDownLatch(1);

        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, IngestRequest ingestRequest) {
                long l = outstandingRequests.getAndIncrement();
                long v = volumeCounter.addAndGet(ingestRequest.estimatedSizeInBytes());
                logger.info("new bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        executionId, ingestRequest.numberOfActions(), v, l);
                idHolder.executionId(executionId);
                latch.countDown();
            }

            @Override
            public void afterBulk(long executionId, IngestResponse response) {
                long l = outstandingRequests.decrementAndGet();
                logger.info("bulk [{}] [{} items succeeded] [{} items failed] [{}ms]",
                        executionId, response.success().size(), response.failure().size(), response.took().millis());
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("bulk [{}] [{} failure reason: {}", executionId, f.id(), f.message());
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, Throwable failure) {
                long l = outstandingRequests.decrementAndGet();
                logger.error("bulk [{}] error", executionId, failure);
                synchronized (responses) {
                    responses.put(executionId, failure);
                }
            }
        };

        String replicationType = request.param("replication");
        if (replicationType != null) {
            ingestProcessor.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            ingestProcessor.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        ingestProcessor.refresh(request.paramAsBoolean("refresh", ingestProcessor.refresh()));
        try {
            long t0 = System.currentTimeMillis();
            ingestProcessor.add(request.content(), request.contentUnsafe(),
                    request.param("index"), request.param("type"), listener);
            // estimation, should be enough time to wait for an ID
            boolean b = latch.await(100, TimeUnit.MILLISECONDS);
            long t1 = System.currentTimeMillis();

            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            builder.field(Fields.OK, true);
            builder.field(Fields.TOOK, t1 - t0);
            // got ID?
            if (b) {
                builder.field(Fields.ID, idHolder.executionId());
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString ID = new XContentBuilderString("id");
    }

    static final class ExecutionIdHolder {
        private long executionId;
        public void executionId(long executionId) {
            this.executionId = executionId;
        }
        public long executionId() {
            return executionId;
        }
    }

    static final class LRUHashMap<K, V> extends LinkedHashMap<K, V> {

        private final int maxSize;

        /**
         * Constructor.
         *
         * @param initialSize initial cache size.
         * @param maxSize maximum cache size.
         */
        public LRUHashMap(final int initialSize, final int maxSize) {
            super(initialSize);
            this.maxSize = maxSize;
        }

        /**
         * Constructor.
         * Equivalent to calling LRUHashMap(size, size);
         *
         * @param size initial and maximum cache size.
         */
        public LRUHashMap(final int size) {
            this(size, size);
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
            return size() >= this.maxSize;
        }
    }

}

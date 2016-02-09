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
package org.xbib.elasticsearch.rest.action.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.xbib.elasticsearch.action.ingest.IngestActionFailure;
import org.xbib.elasticsearch.helper.client.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;

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

    /**
     * Count the volume
     */
    public final static AtomicLong volumeCounter = new AtomicLong();
    private final static ESLogger logger = Loggers.getLogger(RestIngestAction.class);
    /**
     * The IngestProcessor
     */
    private final IngestProcessor ingestProcessor;

    @Inject
    public RestIngestAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(POST, "/_ingest", this);
        controller.registerHandler(PUT, "/_ingest", this);
        controller.registerHandler(POST, "/{index}/_ingest", this);
        controller.registerHandler(PUT, "/{index}/_ingest", this);
        controller.registerHandler(POST, "/{index}/{type}/_ingest", this);
        controller.registerHandler(PUT, "/{index}/{type}/_ingest", this);

        int actions = settings.getAsInt("action.ingest.maxactions", 1000);
        int concurrency = settings.getAsInt("action.ingest.maxconcurrency",
                Runtime.getRuntime().availableProcessors() * 4);
        ByteSizeValue volume = settings.getAsBytesSize("action.ingest.maxvolume",
                ByteSizeValue.parseBytesSizeValue("10m", "action.ingest.maxvolume"));

        this.ingestProcessor = new IngestProcessor(client)
                .maxActions(actions)
                .maxConcurrentRequests(concurrency)
                .maxVolumePerRequest(volume);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final IngestIdHolder idHolder = new IngestIdHolder();
        final CountDownLatch latch = new CountDownLatch(1);
        IngestProcessor.IngestListener ingestListener = new IngestProcessor.IngestListener() {
            @Override
            public void onRequest(int concurrency, IngestRequest ingestRequest) {
                long v = volumeCounter.addAndGet(ingestRequest.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("ingest request [{}] of {} items, {} bytes, {} concurrent requests",
                            ingestRequest.ingestId(), ingestRequest.numberOfActions(), v, concurrency);
                }
                idHolder.ingestId(ingestRequest.ingestId());
                latch.countDown();
            }

            @Override
            public void onResponse(int concurrency, IngestResponse response) {
                if (logger.isDebugEnabled()) {
                    logger.debug("ingest response [{}] [{} succeeded] [{} failed] [{}ms]",
                            response.ingestId(),
                            response.successSize(),
                            response.getFailures().size(),
                            response.tookInMillis());
                }
                if (!response.getFailures().isEmpty()) {
                    for (IngestActionFailure f : response.getFailures()) {
                        logger.error("ingest [{}] failure, reason: {}", response.ingestId(), f.message());
                    }
                }
            }

            @Override
            public void onFailure(int concurrency, long ingestId, Throwable failure) {
                logger.error("ingest [{}] error", ingestId, failure);
            }
        };
        try {
            long t0 = System.currentTimeMillis();
            ingestProcessor.add(request.content(), request.param("index"), request.param("type"), ingestListener);
            // estimation, should be enough time to wait for an ID
            boolean b = latch.await(100, TimeUnit.MILLISECONDS);
            long t1 = System.currentTimeMillis();
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder.field("took", t1 - t0);
            // got ID?
            if (b) {
                builder.field("id", idHolder.ingestId());
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (Exception e) {
            try {
                XContentBuilder builder = jsonBuilder();
                builder.startObject().field("error", e.getMessage()).endObject();
                channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    static final class IngestIdHolder {
        private long ingestId;

        public void ingestId(long ingestId) {
            this.ingestId = ingestId;
        }

        public long ingestId() {
            return ingestId;
        }
    }

}

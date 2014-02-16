
package org.xbib.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.xbib.elasticsearch.action.ingest.IngestItemFailure;
import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;

import java.io.IOException;
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
     * Count the volume
     */
    public final static AtomicLong volumeCounter = new AtomicLong();
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

        int actions = settings.getAsInt("action.ingest.maxactions", 1000);
        int concurrency = settings.getAsInt("action.ingest.maxconcurrency", Runtime.getRuntime().availableProcessors() * 4);
        ByteSizeValue volume = settings.getAsBytesSize("action.ingest.maxvolume", ByteSizeValue.parseBytesSizeValue("10m"));
        TimeValue waitingTime = settings.getAsTime("action.ingest.waitingtime", TimeValue.timeValueSeconds(60));

        this.ingestProcessor = new IngestProcessor(client, concurrency, actions, volume, waitingTime);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {

        final BulkIdHolder idHolder = new BulkIdHolder();
        final CountDownLatch latch = new CountDownLatch(1);

        IngestProcessor.Listener listener = new IngestProcessor.Listener() {
            @Override
            public void beforeBulk(long bulkId, int concurrency, IngestRequest ingestRequest) {
                long v = volumeCounter.addAndGet(ingestRequest.estimatedSizeInBytes());
                if (logger.isDebugEnabled()) {
                    logger.debug("before bulk [{}] of {} items, {} bytes, {} outstanding bulk requests",
                        bulkId, ingestRequest.numberOfActions(), v, concurrency);
                }
                idHolder.bulkId(bulkId);
                latch.countDown();
            }

            @Override
            public void afterBulk(long bulkId, int concurrency, IngestResponse response) {
                if (logger.isDebugEnabled()) {
                    logger.debug("after bulk [{}] [{} items succeeded] [{} items failed] [{}ms]",
                            bulkId, response.successSize(), response.failureSize(), response.took().millis());
                }
                if (!response.failure().isEmpty()) {
                    for (IngestItemFailure f: response.failure()) {
                        logger.error("bulk [{}] [{} failure reason: {}", bulkId, f.pos(), f.message());
                    }
                }
            }

            @Override
            public void afterBulk(long bulkId, int concurrency, Throwable failure) {
                logger.error("bulk [{}] error", bulkId, failure);
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
        try {
            long t0 = System.currentTimeMillis();
            ingestProcessor.add(request.content().toBytes(),
                    request.contentUnsafe(),
                    request.param("index"),
                    request.param("type"),
                    listener);
            // estimation, should be enough time to wait for an ID
            boolean b = latch.await(100, TimeUnit.MILLISECONDS);
            long t1 = System.currentTimeMillis();

            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            builder.field(Fields.TOOK, t1 - t0);
            // got ID?
            if (b) {
                builder.field(Fields.ID, idHolder.bulkId());
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
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString ID = new XContentBuilderString("id");
    }

    static final class BulkIdHolder {
        private long bulkId;
        public void bulkId(long bulkId) {
            this.bulkId = bulkId;
        }
        public long bulkId() {
            return bulkId;
        }
    }

}

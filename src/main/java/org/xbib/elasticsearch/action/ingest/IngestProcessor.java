package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class IngestProcessor {

    private final Client client;

    private int maxConcurrency = Runtime.getRuntime().availableProcessors() * 2;

    private int actions = 1000;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private Semaphore semaphore = new Semaphore(maxConcurrency);

    private AtomicLong ingestId = new AtomicLong(0L);

    private IngestRequest ingestRequest = new IngestRequest();

    private IngestListener ingestListener;

    private ScheduledThreadPoolExecutor scheduler;

    private ScheduledFuture scheduledFuture;

    private volatile boolean closed = false;

    public IngestProcessor(Client client) {
        this.client = client;
    }

    public IngestProcessor maxConcurrentRequests(int concurrency) {
        this.maxConcurrency = Math.min(Math.abs(concurrency < 1 ? 1 : concurrency), 256);
        this.semaphore = new Semaphore(this.maxConcurrency);
        return this;
    }

    public int getConcurrency() {
        return maxConcurrency - semaphore.availablePermits();
    }

    public IngestProcessor maxActions(int actions) {
        this.actions = Math.min(actions, 32768);
        return this;
    }

    public IngestProcessor maxVolumePerRequest(ByteSizeValue maxVolume) {
        this.maxVolume = new ByteSizeValue(Math.max(maxVolume.bytes(), 1024), ByteSizeUnit.BYTES);
        return this;
    }


    public IngestProcessor flushInterval(TimeValue flushInterval) {
        if (flushInterval != null && flushInterval.getMillis() > 0L) {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory((client).settings(), "ingest_processor"));
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
            scheduledFuture = scheduler.scheduleWithFixedDelay(new FlushHelper(), flushInterval.millis(), flushInterval.millis(), TimeUnit.MILLISECONDS);
        }
        return this;
    }

    public IngestProcessor ingestId(long ingestId) {
        this.ingestId = new AtomicLong(ingestId);
        return this;
    }

    public IngestProcessor listener(IngestListener ingestListener) {
        this.ingestListener = ingestListener;
        return this;
    }

    public IngestProcessor add(ActionRequest request) {
        ingestRequest.add(request);
        flushIfNeeded(ingestListener);
        return this;
    }

    /**
     * For REST API
     *
     * @param data           the REST body data
     * @param defaultIndex   default index
     * @param defaultType    default type
     * @param ingestListener the listener
     * @return this processor
     * @throws Exception
     */
    public IngestProcessor add(BytesReference data,
                               @Nullable String defaultIndex, @Nullable String defaultType,
                               IngestListener ingestListener) throws Exception {
        ingestRequest.add(data, defaultIndex, defaultType);
        flushIfNeeded(ingestListener);
        return this;
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it is shut down.
     * Any remaining ingest actions are flushed.
     */
    public void close() throws InterruptedException {
        if (closed) {
            throw new ElasticsearchIllegalStateException("already closed");
        }
        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        // do not automatically flush
        scheduler.shutdown();
        // flush manually but do not wait for responses
        flush();
    }

    /**
     * Flush this processor, write all requests
     */
    public synchronized void flush() {
        if (ingestRequest.numberOfActions() > 0) {
            process(ingestRequest.takeAll(), ingestListener);
        }
    }

    /**
     * Wait for responses of outstanding requests.
     *
     * @return true if all requests answered within the waiting time, false if not
     * @throws InterruptedException
     */
    public boolean waitForResponses(TimeValue maxWait) throws InterruptedException {
        if (maxConcurrency - semaphore.availablePermits() > 0) {
            semaphore.tryAcquire(maxConcurrency, maxWait.getMillis(), TimeUnit.MILLISECONDS);
            return semaphore.availablePermits() == maxConcurrency;
        } else {
            return true;
        }
    }

    /**
     * Critical phase, check if flushing condition is met and
     * push the part of the requests that is required to push
     */
    private synchronized void flushIfNeeded(IngestListener ingestListener) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("processor already closed");
        }
        if (actions > 0) {
            while (ingestRequest.numberOfActions() >= actions) {
                process(ingestRequest.take(actions), ingestListener);
            }
        } else {
            while (ingestRequest.numberOfActions() > 0
                    && maxVolume.bytesAsInt() > 0
                    && ingestRequest.estimatedSizeInBytes() > maxVolume.bytesAsInt()) {
                process(ingestRequest.takeAll(), ingestListener);
            }
        }
    }

    /**
     * Process an ingest request and send responses via the listener.
     *
     * @param request the ingest request
     */
    private void process(final IngestRequest request, final IngestListener ingestListener) {
        if (ingestListener == null) {
            return;
        }
        request.ingestId(ingestId.incrementAndGet());
        boolean done = false;
        try {
            semaphore.acquire();
            ingestListener.onRequest(maxConcurrency - semaphore.availablePermits(), request);
            client.execute(IngestAction.INSTANCE, request, new ActionListener<IngestResponse>() {
                @Override
                public void onResponse(IngestResponse response) {
                    try {
                        ingestListener.onResponse(maxConcurrency - semaphore.availablePermits(), response);
                    } finally {
                        semaphore.release();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        ingestListener.onFailure(maxConcurrency - semaphore.availablePermits(), request.ingestId(), e);
                    } finally {
                        semaphore.release();
                    }
                }
            });
            done = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ingestListener.onFailure(maxConcurrency - semaphore.availablePermits(), request.ingestId(), e);
        } finally {
            if (!done) {
                semaphore.release();
            }
        }
    }

    class FlushHelper implements Runnable {

        @Override
        public void run() {
            flushIfNeeded(ingestListener);
        }
    }

    /**
     * A listener for ingest executions
     */
    public static interface IngestListener {

        /**
         * Called before the ingest request is executed.
         */
        void onRequest(int concurrency, IngestRequest request);

        /**
         * Called after a successful execution of an ingest request.
         */
        void onResponse(int concurrency, IngestResponse response);

        /**
         * Callback after a failed execution of an ingest request.
         */
        void onFailure(int concurrency, long ingestId, Throwable failure);
    }

}

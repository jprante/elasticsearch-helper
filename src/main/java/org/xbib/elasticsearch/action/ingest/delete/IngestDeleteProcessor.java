
package org.xbib.elasticsearch.action.ingest.delete;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class IngestDeleteProcessor {

    private final Client client;

    private final int concurrency;

    private final int actions;

    private final ByteSizeValue maxVolume;

    private final TimeValue waitForResponses;

    private final Semaphore semaphore;

    private final AtomicLong bulkId;

    private final IngestDeleteRequest ingestRequest;

    private Listener listener;

    private volatile boolean closed = false;

    public IngestDeleteProcessor(Client client, Integer concurrency, Integer actions,
                                 ByteSizeValue maxVolume, TimeValue waitForResponses) {
        this.client = client;
        this.concurrency = concurrency != null ?
                Math.min(Math.abs(concurrency), 256) :
                Runtime.getRuntime().availableProcessors() * 4;
        this.actions = actions != null ? Math.min(actions, 32768) : 1000;
        this.maxVolume = maxVolume != null ?
                new ByteSizeValue(Math.max(maxVolume.bytes(), 1024), ByteSizeUnit.BYTES) :
                new ByteSizeValue(10, ByteSizeUnit.MB);
        this.waitForResponses = waitForResponses != null ?
                new TimeValue(Math.max(waitForResponses.millis(), 1000), TimeUnit.MILLISECONDS) :
                new TimeValue(60, TimeUnit.SECONDS);
        this.semaphore = new Semaphore(this.concurrency);
        this.bulkId = new AtomicLong(0L);
        this.ingestRequest = new IngestDeleteRequest();
    }

    public IngestDeleteProcessor listener(Listener listener) {
        this.listener = listener;
        return this;
    }

    public IngestDeleteProcessor listenerThreaded(boolean threaded) {
        ingestRequest.listenerThreaded(threaded);
        return this;
    }

    public IngestDeleteProcessor replicationType(ReplicationType type) {
        ingestRequest.replicationType(type);
        return this;
    }

    public IngestDeleteProcessor consistencyLevel(WriteConsistencyLevel level) {
        ingestRequest.consistencyLevel(level);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list
     * of actions to execute.
     */
    public IngestDeleteProcessor add(DeleteRequest request) {
        ingestRequest.add(request);
        flushIfNeeded(listener);
        return this;
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it is shut down.
     * Any remaining bulk actions are flushed, and for the bulk responses is being waited.
     */
    public synchronized void close() throws InterruptedException {
        if (closed) {
            return;
        }
        closed = true;
        flush();
        waitForResponses(waitForResponses);
    }

    /**
     * Flush this bulk processor, write all requests
     */
    public synchronized void flush() {
        if (ingestRequest.numberOfActions() > 0) {
            process(ingestRequest.takeAll(), listener);
        }
    }

    /**
     * Critical phase, check if flushing condition is met and
     * push the part of the bulk requests that is required to push
     */
    private synchronized void flushIfNeeded(Listener listener) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("ingest processor already closed");
        }
        if (actions > 0) {
            if (ingestRequest.numberOfActions() >= actions) {
                process(ingestRequest.take(actions), listener);
            }
        } else {
            if (ingestRequest.numberOfActions() > 0
                    && maxVolume.bytes() > 0
                    && ingestRequest.estimatedSizeInBytes() > maxVolume.bytes()) {
                process(ingestRequest.takeAll(), listener);
            }
        }
    }

    /**
     * A thread can flush a partial ConcurrentBulkRequest here.
     * Check for concurrency limit and wait for outstanding requests by the help
     * of a semaphore.
     *
     * @param request the ingest request
     */
    private void process(final IngestDeleteRequest request, final Listener listener) {
        if (request.numberOfActions() == 0) {
            return;
        }
        if (listener == null) {
            return;
        }
        final long id = bulkId.incrementAndGet();
        boolean done = false;
        try {
            semaphore.acquire();
            listener.beforeBulk(id, concurrency - semaphore.availablePermits(), request);
            client.execute(IngestDeleteAction.INSTANCE, request, new ActionListener<IngestResponse>() {
                @Override
                public void onResponse(IngestResponse response) {
                    try {
                        listener.afterBulk(id, concurrency - semaphore.availablePermits(), response);
                    } finally {
                        semaphore.release();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        listener.afterBulk(id, concurrency - semaphore.availablePermits(), e);
                    } finally {
                        semaphore.release();
                    }
                }
            });
            done = true;
        } catch (InterruptedException e) {
            // semaphore not acquired
            Thread.currentThread().interrupt();
            listener.afterBulk(id, concurrency - semaphore.availablePermits(), e);
        } finally {
            if (!done) {
                semaphore.release();
            }
        }
    }

    /**
     * Wait for responses of outstanding requests.
     *
     * @return true if all requests answered within the waiting time, false if not
     * @throws InterruptedException
     */
    public boolean waitForResponses(TimeValue maxWait) throws InterruptedException {
        semaphore.tryAcquire(concurrency, maxWait.getMillis(), TimeUnit.MILLISECONDS);
        return semaphore.availablePermits() == concurrency;
    }

    /**
     * A listener for the execution.
     */
    public static interface Listener {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(long bulkId, int concurrency, IngestDeleteRequest request);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(long bulkId, int concurrency, IngestResponse response);

        /**
         * Callback after a failed execution of bulk request.
         */
        void afterBulk(long bulkId, int concurrency, Throwable failure);
    }

}

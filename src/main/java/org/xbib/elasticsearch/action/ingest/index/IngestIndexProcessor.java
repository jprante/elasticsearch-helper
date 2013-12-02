
package org.xbib.elasticsearch.action.ingest.index;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.ByteSizeUnit;

import org.xbib.elasticsearch.action.ingest.IngestResponse;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class IngestIndexProcessor {

    private final Client client;

    private final int concurrency;

    private final int actions;

    private final ByteSizeValue maxVolume;

    private final Semaphore semaphore;

    private final AtomicLong executionIdGen;

    private final IngestIndexRequest ingestRequest;

    private Listener listener;

    private volatile boolean closed = false;

    public IngestIndexProcessor(Client client, Integer concurrency, Integer actions, ByteSizeValue maxVolume) {
        this.client = client;
        this.concurrency = concurrency != null ? concurrency > 0 ? concurrency : -concurrency :
                Runtime.getRuntime().availableProcessors() * 8;
        this.actions = actions != null ? actions : 1000;
        this.maxVolume = maxVolume != null ? maxVolume : new ByteSizeValue(10, ByteSizeUnit.MB);
        this.semaphore = new Semaphore(this.concurrency);
        this.executionIdGen = new AtomicLong(0L);
        this.ingestRequest = new IngestIndexRequest();

    }

    public IngestIndexProcessor listener(Listener listener) {
        this.listener = listener;
        return this;
    }

    public IngestIndexProcessor listenerThreaded(boolean threaded) {
        ingestRequest.listenerThreaded(threaded);
        return this;
    }

    public IngestIndexProcessor replicationType(ReplicationType type) {
        ingestRequest.replicationType(type);
        return this;
    }

    public IngestIndexProcessor consistencyLevel(WriteConsistencyLevel level) {
        ingestRequest.consistencyLevel(level);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list
     * of actions to execute.
     */
    public IngestIndexProcessor add(IndexRequest request) {
        ingestRequest.add(request);
        flushIfNeeded();
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
        waitForAllResponses();
    }

    /**
     * Flush this bulk processor, write all requests
     */
    public void flush() {
        synchronized (ingestRequest) {
            if (ingestRequest.numberOfActions() > 0) {
                process(ingestRequest.takeAll(), listener);
            }
        }
    }

    private void flushIfNeeded() {
        flushIfNeeded(listener);
    }

    /**
     * Critical phase, check if flushing condition is met and
     * push the part of the bulk requests that is required to push
     */
    private void flushIfNeeded(Listener listener) {
        if (closed) {
            throw new ElasticSearchIllegalStateException("ingest processor already closed");
        }
        synchronized (ingestRequest) {
            if (actions > 0 && ingestRequest.numberOfActions() >= actions) {
                process(ingestRequest.take(actions), listener);
            } else if (maxVolume.bytesAsInt() > 0 && ingestRequest.estimatedSizeInBytes() >= maxVolume.bytesAsInt()) {
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
     * @param listener the listener
     */
    private void process(final IngestIndexRequest request, final Listener listener) {
        if (request.numberOfActions() == 0) {
            return;
        }
        // only works with a listener
        if (listener == null) {
            return;
        }
        final long executionId = executionIdGen.incrementAndGet();
        listener.beforeBulk(executionId, concurrency - semaphore.availablePermits(), request);
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            listener.afterBulk(executionId, concurrency - semaphore.availablePermits(), e);
            return;
        }
        client.execute(IngestIndexAction.INSTANCE, request, new ActionListener<IngestResponse>() {
            @Override
            public void onResponse(IngestResponse response) {
                try {
                    listener.afterBulk(executionId, concurrency - semaphore.availablePermits(), response);
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    listener.afterBulk(executionId, concurrency - semaphore.availablePermits(), e);
                } finally {
                    semaphore.release();
                }
            }
        });
    }

    /**
     * Wait for all outstanding bulk requests.
     * At maximum, the waiting time is 60 seconds.
     *
     * @return true if there are no outstanding bulk requests, false otherwise
     * @throws InterruptedException
     */
    public synchronized boolean waitForAllResponses() throws InterruptedException {
        int n = 60;
        while (semaphore.availablePermits() < concurrency && n > 0) {
            Thread.sleep(1000L);
            n--;
        }
        return semaphore.availablePermits() == concurrency;
    }

    /**
     * A listener for the execution.
     */
    public static interface Listener {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(long executionId, int concurrency, IngestIndexRequest request);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(long executionId, int concurrency, IngestResponse response);

        /**
         * Callback after a failed execution of bulk request.
         */
        void afterBulk(long executionId, int concurrency, Throwable failure);
    }

}

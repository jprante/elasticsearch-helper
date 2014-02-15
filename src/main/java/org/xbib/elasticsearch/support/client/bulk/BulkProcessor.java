
package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p/>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor {

    /**
     * A listener for the execution.
     */
    public static interface Listener {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(long executionId, BulkRequest request);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        /**
         * Callback after a failed execution of bulk request.
         */
        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final Client client;
        private final Listener listener;

        private String name;
        private int concurrentRequests = 1;
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;

        /**
         * Creates a builder of bulk processor with the client to use and the listener that will be used
         * to be notified on the completion of bulk requests.
         */
        public Builder(Client client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        /**
         * Sets an optional name to identify this bulk processor.
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to <tt>1</tt>.
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * <tt>1000</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * <tt>5mb</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
         * <p/>
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to <tt>-1</tt> with the flush interval set allowing for complete async processing of bulk actions.
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkProcessor build() {
            return new BulkProcessor(client, listener, name, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    public static Builder builder(Client client, Listener listener) {
        return new Builder(client, listener);
    }

    private final Client client;
    private final Listener listener;

    private final String name;

    private final int concurrentRequests;
    private final int bulkActions;
    private final TimeValue flushInterval;

    private final Semaphore semaphore;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture scheduledFuture;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;

    private volatile boolean closed = false;

    BulkProcessor(Client client, Listener listener, @Nullable String name, int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval) {
        this.client = client;
        this.listener = listener;
        this.name = name;
        this.concurrentRequests = concurrentRequests;
        this.bulkActions = bulkActions;

        this.semaphore = new Semaphore(concurrentRequests);
        this.bulkRequest = new BulkRequest();

        this.flushInterval = flushInterval;
        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory("bulk processor"));
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(), flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then its shutdown. Any remaining bulk actions are flushed.
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            this.scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(IndexRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds either a delete or an index request.
     */
    public BulkProcessor add(ActionRequest request) {
        return add(request, null);
    }

    public BulkProcessor add(ActionRequest request, @Nullable Object payload) {
        internalAdd(request, payload);
        return this;
    }

    private synchronized void internalAdd(ActionRequest request, @Nullable Object payload) {
        if (request instanceof IndexRequest) {
            bulkRequest.add((IndexRequest)request);
        } else if (request instanceof DeleteRequest) {
            bulkRequest.add((DeleteRequest)request);
        }
        executeIfNeeded();
    }

    public BulkProcessor add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
            bulkRequest.add(data, from, length, contentUnsafe, defaultIndex, defaultType);
        executeIfNeeded();
        return this;
    }

    private void executeIfNeeded() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("bulk process already closed");
        }
        if (!isOverTheLimit()) {
            return;
        }
        execute();
    }

    // (currently) needs to be executed under a lock
    private void execute() {
        final BulkRequest bulkRequest = this.bulkRequest;
        final long executionId = executionIdGen.incrementAndGet();

        this.bulkRequest = new BulkRequest();

        if (concurrentRequests == 0) {
            // execute in a blocking fashion...
            try {
                listener.beforeBulk(executionId, bulkRequest);
                listener.afterBulk(executionId, bulkRequest, client.bulk(bulkRequest).actionGet());
            } catch (Exception e) {
                listener.afterBulk(executionId, bulkRequest, e);
            }
        } else {
            boolean success = false;
            try {
                semaphore.acquire();
                listener.beforeBulk(executionId, bulkRequest);
                client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, response);
                        } finally {
                            semaphore.release();
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, e);
                        } finally {
                            semaphore.release();
                        }
                    }
                });
                success = true;
            } catch (InterruptedException e) {
                Thread.interrupted();
                listener.afterBulk(executionId, bulkRequest, e);
            } finally {
                if (!success) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }

        }
    }

    private boolean isOverTheLimit() {
        if (bulkActions != -1 && bulkRequest.numberOfActions() > bulkActions) {
            return true;
        }
        return false;
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            }
        }
    }
}

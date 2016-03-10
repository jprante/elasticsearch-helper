package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.io.Closeable;
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
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor implements Closeable {

    /**
     * A listener for the execution.
     */
    public interface Listener {

        /**
         * Callback before the bulk is executed.
         * @param executionId execution ID
         * @param request request
         */
        void beforeBulk(long executionId, BulkRequest request);

        /**
         * Callback after a successful execution of bulk request.
         * @param executionId execution ID
         * @param request request
         * @param response response
         */
        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        /**
         * Callback after a failed execution of bulk request.
         *
         * Note that in case an instance of <code>InterruptedException</code> is passed, which means that request processing has been
         * cancelled externally, the thread's interruption status has been restored prior to calling this method.
         * @param executionId execution ID
         * @param request request
         * @param failure failure
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
         * @param client the client
         * @param listener the listener
         */
        public Builder(Client client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        /**
         * Sets an optional name to identify this bulk processor.
         * @param name name
         * @return this builder
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to <tt>1</tt>.
         * @param concurrentRequests maximum number of concurrent requests
         * @return this builder
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * <tt>1000</tt>. Can be set to <tt>-1</tt> to disable it.
         * @param bulkActions mbulk actions
         * @return this builder
         */
        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * <tt>5mb</tt>. Can be set to <tt>-1</tt> to disable it.
         * @param bulkSize bulk size
         * @return this builder
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to <tt>-1</tt> with the flush interval set allowing for complete async processing of bulk actions.
         * @param flushInterval flush interval
         * @return this builder
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Builds a new bulk processor.
         * @return a bulk processor
         */
        public BulkProcessor build() {
            return new BulkProcessor(client, listener, name, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    public static Builder builder(Client client, Listener listener) {
        if (client == null) {
            throw new NullPointerException("The client you specified while building a BulkProcessor is null");
        }
        return new Builder(client, listener);
    }

    private final int bulkActions;
    private final long bulkSize;

    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture scheduledFuture;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;
    private final BulkRequestHandler bulkRequestHandler;

    private volatile boolean closed = false;

    BulkProcessor(Client client, Listener listener, @Nullable String name, int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.bytes();

        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler = concurrentRequests == 0 ?
                new SyncBulkRequestHandler(client, listener) :
                new AsyncBulkRequestHandler(client, listener, concurrentRequests);

        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory(client.settings(), (name != null ? "[" + name + "]" : "") + "bulk_processor"));
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(), flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     */
    @Override
    public void close() {
        try {
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch(InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     *
     * If concurrent requests are not enabled, returns {@code true} immediately.
     * If concurrent requests are enabled, waits for up to the specified timeout for all bulk requests to complete then returns {@code true},
     * If the specified waiting time elapses before all bulk requests complete, {@code false} is returned.
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit The time unit of the {@code timeout} argument
     * @return {@code true} if all bulk requests completed and {@code false} if the waiting time elapsed before all the bulk requests completed
     * @throws InterruptedException If the current thread is interrupted
     */
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return true;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            FutureUtils.cancel(this.scheduledFuture);
            this.scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return this.bulkRequestHandler.awaitClose(timeout, unit);
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     * @param request request
     * @return his bulk processor
     */
    public BulkProcessor add(IndexRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     * @param request request
     * @return his bulk processor
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds either a delete or an index request.
     * @param request request
     * @return his bulk processor
     */
    public BulkProcessor add(ActionRequest request) {
        return add(request, null);
    }

    /**
     * Adds either a delete or an index request with a payload.
     * @param request request
     * @param payload payload
     * @return his bulk processor
     */
    public BulkProcessor add(ActionRequest request, @Nullable Object payload) {
        internalAdd(request, payload);
        return this;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private synchronized void internalAdd(ActionRequest request, @Nullable Object payload) {
        ensureOpen();
        bulkRequest.add(request, payload);
        executeIfNeeded();
    }

    public BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(data, defaultIndex, defaultType, null);
    }

    public synchronized BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable Object payload) throws Exception {
        bulkRequest.add(data, defaultIndex, defaultType, null, null, payload, true);
        executeIfNeeded();
        return this;
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (!isOverTheLimit()) {
            return;
        }
        execute();
    }

    private void execute() {
        final BulkRequest bulkRequest = this.bulkRequest;
        final long executionId = executionIdGen.incrementAndGet();

        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    private boolean isOverTheLimit() {
        return bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions || bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize;
    }

    /**
     * Flush pending delete or index requests.
     */
    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
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

    /**
     * Abstracts the low-level details of bulk request handling
     */
    abstract class BulkRequestHandler {

        public abstract void execute(BulkRequest bulkRequest, long executionId);

        public abstract boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

    }

    class SyncBulkRequestHandler extends BulkRequestHandler {
        private final Client client;
        private final BulkProcessor.Listener listener;

        public SyncBulkRequestHandler(Client client, BulkProcessor.Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = client.execute(BulkAction.INSTANCE, bulkRequest).actionGet();
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkResponse);
            } catch (Throwable t) {
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, t);
                }
            }
        }

        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }
    }

    class AsyncBulkRequestHandler extends BulkRequestHandler {
        private final Client client;
        private final BulkProcessor.Listener listener;
        private final Semaphore semaphore;
        private final int concurrentRequests;

        private AsyncBulkRequestHandler(Client client, BulkProcessor.Listener listener, int concurrentRequests) {
            this.client = client;
            this.listener = listener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(final BulkRequest bulkRequest, final long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                client.execute(BulkAction.INSTANCE, bulkRequest, new ActionListener<BulkResponse>() {
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
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Throwable t) {
                listener.afterBulk(executionId, bulkRequest, t);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                semaphore.release(this.concurrentRequests);
                return true;
            }
            return false;
        }
    }
}

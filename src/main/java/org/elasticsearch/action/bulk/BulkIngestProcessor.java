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
package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily
 * set when to "flush" a new bulk request (either based on number of actions,
 * based on the size, or time), and to easily control the number of concurrent
 * bulk requests allowed to be executed in parallel.
 *
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkIngestProcessor {

    private final Client client;
    private final int concurrency;
    private final int actions;
    private final int maxVolume;
    private final Semaphore semaphore;
    private final AtomicLong executionIdGen = new AtomicLong();
    private final BulkIngestRequest ingestRequest = new BulkIngestRequest();
    private Listener listener;
    private volatile boolean closed = false;

    public static Builder builder(Client client) {
        return new Builder(client);
    }

    BulkIngestProcessor(Client client, int concurrency, int actions, ByteSizeValue maxVolume) {
        this.client = client;
        this.concurrency = concurrency;
        this.actions = actions;
        this.maxVolume = maxVolume.bytesAsInt();
        this.semaphore = new Semaphore(concurrency);
    }

    public BulkIngestProcessor listener(Listener listener) {
        this.listener = listener;
        return this;
    }

    public BulkIngestProcessor listenerThreaded(boolean threaded) {
        ingestRequest.listenerThreaded(threaded);
        return this;
    }

    public BulkIngestProcessor replicationType(ReplicationType type) {
        ingestRequest.replicationType(type);
        return this;
    }

    public BulkIngestProcessor consistencyLevel(WriteConsistencyLevel level) {
        ingestRequest.consistencyLevel(level);
        return this;
    }

    public BulkIngestProcessor refresh(boolean refresh) {
        ingestRequest.refresh(refresh);
        return this;
    }

    public boolean refresh() {
        return ingestRequest.refresh();
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list
     * of actions to execute. Follows the same behavior of
     * {@link org.elasticsearch.action.index.IndexRequest} (for example, if no
     * id is provided, one will be generated, or usage of the create flag).
     */
    public BulkIngestProcessor add(IndexRequest request) {
        ingestRequest.add((ActionRequest) request);
        flushIfNeeded();
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list
     * of actions to execute.
     */
    public BulkIngestProcessor add(DeleteRequest request) {
        ingestRequest.add((ActionRequest) request);
        flushIfNeeded();
        return this;
    }

    public BulkIngestProcessor add(BytesReference data, boolean contentUnsafe,
                               @Nullable String defaultIndex, @Nullable String defaultType,
                               Listener listener) throws Exception {
        ingestRequest.add(data, contentUnsafe, defaultIndex, defaultType);
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
            if (maxVolume > 0 && ingestRequest.estimatedSizeInBytes() >= maxVolume) {
                process(ingestRequest.takeAll(), listener);
            } else {
                if (actions > 0 && ingestRequest.numberOfActions() >= actions) {
                    process(ingestRequest.take(actions), listener);
                }
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
    private void process(final BulkIngestRequest request, final Listener listener) {
        if (request.numberOfActions() == 0) {
            return;
        }
        // only works with a listener
        if (listener == null) {
            return;
        }

        final long executionId = executionIdGen.incrementAndGet();

        if (concurrency <= 0) {
            // execute in a blocking fashion...
            try {
                listener.beforeBulk(executionId, request);
                listener.afterBulk(executionId, client.bulk(request).actionGet());
            } catch (Exception e) {
                listener.afterBulk(executionId, e);
            }
        } else {
            listener.beforeBulk(executionId, request);

            // concurrency control
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                listener.afterBulk(executionId, e);
                return;
            }

            client.bulk(request, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    try {
                        listener.afterBulk(executionId, response);
                    } finally {
                        semaphore.release();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        listener.afterBulk(executionId, e);
                    } finally {
                        semaphore.release();
                    }
                }
            });
        }
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
        void beforeBulk(long executionId, BulkIngestRequest request);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(long executionId, BulkResponse response);

        /**
         * Callback after a failed execution of bulk request.
         */
        void afterBulk(long executionId, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final Client client;
        private Listener listener;
        private int concurrency = 30;
        private int actions = 100;
        private ByteSizeValue volume = new ByteSizeValue(5, ByteSizeUnit.MB);

        /**
         * Creates a builder of bulk processor with the client to use
         */
        public Builder(Client client) {
            this.client = client;
        }

        /**
         *  Set the listener that will be used to be notified on the completion of bulk requests.
         * @param listener
         * @return the builder
         */
        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A
         * value of 0 means that only a single request will be allowed to be
         * executed. A value of 1 means 1 concurrent request is allowed to be
         * executed while accumulating new bulk requests. Defaults to
         * <tt>1</tt>.
         */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions
         * currently added. Defaults to <tt>1000</tt>. Can be set to <tt>-1</tt>
         * to disable it.
         */
        public Builder actions(int actions) {
            this.actions = actions;
            return this;
        }

        /**
         * When to flush a new bulk request based on the byte volume of actions
         * currently added. Defaults to <tt>5mb</tt>. Can be set to <tt>-1</tt>
         * to disable it.
         */
        public Builder maxVolume(ByteSizeValue volume) {
            this.volume = volume;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkIngestProcessor build() {
            return new BulkIngestProcessor(client, concurrency, actions, volume)
                    .listener(listener);
        }
    }
}

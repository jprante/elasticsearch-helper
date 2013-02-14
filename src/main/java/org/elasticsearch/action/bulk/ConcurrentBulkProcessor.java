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
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily
 * set when to "flush" a new bulk request (either based on number of actions,
 * based on the size, or time), and to easily control the number of concurrent
 * bulk requests allowed to be executed in parallel.
 * <p/>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class ConcurrentBulkProcessor {

    private final Client client;
    private final Listener listener;
    private final int maxConcurrentBulkRequests;
    private final int maxBulkActions;
    private final int maxBulkSize;
    private final Semaphore semaphore;
    private final AtomicLong executionIdGen = new AtomicLong();
    private final ConcurrentBulkRequest bulkRequest = new ConcurrentBulkRequest();
    private volatile boolean closed = false;

    ConcurrentBulkProcessor(Client client, Listener listener, int maxConcurrentBulkRequests, int maxBulkActions, ByteSizeValue maxBulkSize) {
        this.client = client;
        this.listener = listener;
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        this.maxBulkActions = maxBulkActions;
        this.maxBulkSize = maxBulkSize.bytesAsInt();
        this.semaphore = new Semaphore(maxConcurrentBulkRequests);
    }

    public static Builder builder(Client client, Listener listener) {
        return new Builder(client, listener);
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
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list
     * of actions to execute. Follows the same behavior of
     * {@link org.elasticsearch.action.index.IndexRequest} (for example, if no
     * id is provided, one will be generated, or usage of the create flag).
     */
    public ConcurrentBulkProcessor add(IndexRequest request) {
        bulkRequest.add((ActionRequest) request);
        flushIfNeeded();
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list
     * of actions to execute.
     */
    public ConcurrentBulkProcessor add(DeleteRequest request) {
        bulkRequest.add((ActionRequest) request);
        flushIfNeeded();
        return this;
    }

    /**
     * Flush this bulk processor, write all requests
     */
    public void flush() {
        synchronized (bulkRequest) {
            if (bulkRequest.numberOfActions() > 0) {
                flush(bulkRequest.takeAll());
            }
        }
    }

    /**
     * Critical phase, check if flushing condition is met and
     * push the part of the bulk requests that is required to push
     */
    private void flushIfNeeded() {
        if (closed) {
            throw new ElasticSearchIllegalStateException("bulk process already closed");
        }
        synchronized (bulkRequest) {
            if (maxBulkSize > 0 && bulkRequest.estimatedSizeInBytes() >= maxBulkSize) {
                flush(bulkRequest.takeAll());
            } else {
                if (maxBulkActions > 0 && bulkRequest.numberOfActions() >= maxBulkActions) {
                    flush(bulkRequest.take(maxBulkActions));
                }
            }
        }
    }

    /**
     * A thread can flush a partial ConcurrentBulkRequest here.
     * Check for maximum concurrency and wait for outstanding requests by the help
     * of a semaphore.
     *
     * @param request
     */
    private void flush(final ConcurrentBulkRequest request) {
        if (request.numberOfActions() == 0) {
            return;
        }
        final long executionId = executionIdGen.incrementAndGet();

        if (maxConcurrentBulkRequests <= 0) {
            // execute in a blocking fashion...
            try {
                listener.beforeBulk(executionId, request);
                listener.afterBulk(executionId, client.bulk(request).actionGet());
            } catch (Exception e) {
                listener.afterBulk(executionId, e);
            }
        } else {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                listener.afterBulk(executionId, e);
                return;
            }
            listener.beforeBulk(executionId, request);
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

    public void waitForAllResponses() throws InterruptedException {
        int n = 60;
        while (semaphore.availablePermits() < maxConcurrentBulkRequests && n > 0) {
            Thread.sleep(1000L);
            n--;
        }
    }

    /**
     * A listener for the execution.
     */
    public static interface Listener {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(long executionId, ConcurrentBulkRequest request);

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
        private final Listener listener;
        private int maxConcurrentBulkRequests = 10;
        private int maxBulkActions = 100;
        private ByteSizeValue maxBulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);

        /**
         * Creates a builder of bulk processor with the client to use and the
         * listener that will be used to be notified on the completion of bulk
         * requests.
         */
        public Builder(Client client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A
         * value of 0 means that only a single request will be allowed to be
         * executed. A value of 1 means 1 concurrent request is allowed to be
         * executed while accumulating new bulk requests. Defaults to
         * <tt>1</tt>.
         */
        public Builder maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
            this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions
         * currently added. Defaults to <tt>1000</tt>. Can be set to <tt>-1</tt>
         * to disable it.
         */
        public Builder maxBulkActions(int maxBulkActions) {
            this.maxBulkActions = maxBulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions
         * currently added. Defaults to <tt>5mb</tt>. Can be set to <tt>-1</tt>
         * to disable it.
         */
        public Builder maxBulkSize(ByteSizeValue maxBulkSize) {
            this.maxBulkSize = maxBulkSize;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public ConcurrentBulkProcessor build() {
            return new ConcurrentBulkProcessor(client, listener, maxConcurrentBulkRequests, maxBulkActions, maxBulkSize);
        }
    }
}

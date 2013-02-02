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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;

/**
 * A bulk request holds an ordered {@link org.elasticsearch.action.index.IndexRequest}s
 * and {@link org.elasticsearch.action.delete.DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class ConcurrentBulkRequestBuilder extends ActionRequestBuilder<ConcurrentBulkRequest, BulkResponse, ConcurrentBulkRequestBuilder> {

    public ConcurrentBulkRequestBuilder(Client client) {
        super((InternalClient) client, new ConcurrentBulkRequest());
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public ConcurrentBulkRequestBuilder add(IndexRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public ConcurrentBulkRequestBuilder add(IndexRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public ConcurrentBulkRequestBuilder add(DeleteRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public ConcurrentBulkRequestBuilder add(DeleteRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public ConcurrentBulkRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        request.add(data, from, length, contentUnsafe, null, null);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public ConcurrentBulkRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        request.add(data, from, length, contentUnsafe, defaultIndex, defaultType);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public ConcurrentBulkRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public ConcurrentBulkRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Should a refresh be executed post this bulk operation causing the operations to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public ConcurrentBulkRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }

    @Override
    protected void doExecute(ActionListener<BulkResponse> listener) {
        ((Client) client).bulk(request, listener);
    }
}

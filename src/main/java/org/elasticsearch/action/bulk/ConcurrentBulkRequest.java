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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Queues;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ConcurrentBulkRequest extends BulkRequest {

    private static final int REQUEST_OVERHEAD = 50;
    protected final Queue<ActionRequest> requests = Queues.newConcurrentLinkedQueue();
    private boolean listenerThreaded = false;
    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;
    private boolean refresh = false;
    private final AtomicLong sizeInBytes = new AtomicLong();

    public ConcurrentBulkRequest requests(Collection<ActionRequest> requests, long sizeInBytes) {
        this.requests.addAll(requests);
        this.sizeInBytes.set(sizeInBytes);
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public ConcurrentBulkRequest add(ActionRequest... requests) {
        for (ActionRequest request : requests) {
            add(request);
        }
        return this;
    }

    public ConcurrentBulkRequest add(ActionRequest request) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request);
        } else {
            throw new ElasticSearchIllegalArgumentException("No support for request [" + request + "]");
        }
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public ConcurrentBulkRequest add(Iterable<ActionRequest> requests) {
        for (ActionRequest request : requests) {
            if (request instanceof IndexRequest) {
                add((IndexRequest) request);
            } else if (request instanceof DeleteRequest) {
                add((DeleteRequest) request);
            } else {
                throw new ElasticSearchIllegalArgumentException("No support for request [" + request + "]");
            }
        }
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows
     * the same behavior of {@link org.elasticsearch.action.index.IndexRequest} (for example, if no id is
     * provided, one will be generated, or usage of the create flag).
     */
    public ConcurrentBulkRequest add(IndexRequest request) {
        request.beforeLocalFork();
        return internalAdd(request);
    }

    ConcurrentBulkRequest internalAdd(IndexRequest request) {
        requests.add(request);
        sizeInBytes.addAndGet(request.source().length() + REQUEST_OVERHEAD);
        return this;
    }

    /**
     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
     */
    public ConcurrentBulkRequest add(DeleteRequest request) {
        requests.add(request);
        sizeInBytes.addAndGet(REQUEST_OVERHEAD);
        return this;
    }

    /**
     * The number of actions in the bulk request.
     */
    public int numberOfActions() {
        // for ConcurrentLinkedList, this call is not O(n), and may not be the size of the current list
        return requests.size();
    }

    /**
     * The estimated size in bytes of the bulk request.
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes.longValue();
    }

    /**
     * Adds a framed data in binary format
     */
    public ConcurrentBulkRequest add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        return add(data, from, length, contentUnsafe, null, null);
    }

    /**
     * Adds a framed data in binary format
     */
    public ConcurrentBulkRequest add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(new BytesArray(data, from, length), contentUnsafe, defaultIndex, defaultType);
    }

    /**
     * Adds a framed data in binary format
     */
    public ConcurrentBulkRequest add(BytesReference data, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            // now parse the move
            XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from));

            try {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                assert token == XContentParser.Token.START_OBJECT;
                // Move to FIELD_NAME, that's the move
                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String routing = null;
                String parent = null;
                String timestamp = null;
                Long ttl = null;
                String opType = null;
                long version = 0;
                VersionType versionType = VersionType.INTERNAL;
                String percolate = null;

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("_index".equals(currentFieldName)) {
                            index = parser.text();
                        } else if ("_type".equals(currentFieldName)) {
                            type = parser.text();
                        } else if ("_id".equals(currentFieldName)) {
                            id = parser.text();
                        } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                            routing = parser.text();
                        } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                            parent = parser.text();
                        } else if ("_timestamp".equals(currentFieldName) || "timestamp".equals(currentFieldName)) {
                            timestamp = parser.text();
                        } else if ("_ttl".equals(currentFieldName) || "ttl".equals(currentFieldName)) {
                            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                                ttl = TimeValue.parseTimeValue(parser.text(), null).millis();
                            } else {
                                ttl = parser.longValue();
                            }
                        } else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
                            opType = parser.text();
                        } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                            version = parser.longValue();
                        } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                            versionType = VersionType.fromString(parser.text());
                        } else if ("percolate".equals(currentFieldName) || "_percolate".equals(currentFieldName)) {
                            percolate = parser.textOrNull();
                        }
                    }
                }

                if ("delete".equals(action)) {
                    add(new DeleteRequest(index, type, id).parent(parent).version(version).versionType(versionType).routing(routing));
                } else {
                    nextMarker = findNextMarker(marker, from, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    // order is important, we set parent after routing, so routing will be set to parent if not set explicitly
                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request. All index requests are still unsafe if applicable.
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .source(data.slice(from, nextMarker - from), contentUnsafe)
                                    .percolate(percolate));
                        } else {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .create("create".equals(opType))
                                    .source(data.slice(from, nextMarker - from), contentUnsafe)
                                    .percolate(percolate));
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                .create(true)
                                .source(data.slice(from, nextMarker - from), contentUnsafe)
                                .percolate(percolate));
                    }
                    // move pointers
                    from = nextMarker + 1;
                }
            } finally {
                parser.close();
            }
        }
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to
     * {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public ConcurrentBulkRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Should a refresh be executed post this bulk operation causing the
     * operations to be searchable. Note, heavy indexing should not set this to
     * <tt>true</tt>. Defaults to <tt>false</tt>.
     */
    public ConcurrentBulkRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Set the replication type for this operation.
     */
    public ConcurrentBulkRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Take all requests out of this bulk request.
     * This method is thread safe.
     *
     * @return another bulk request
     */
    public synchronized ConcurrentBulkRequest takeAll() {
        return take(requests.size());
    }

    /**
     * Take a number of requests out of this bulk request and put them
     * into an array list.
     * <p/>
     * This method is thread safe.
     *
     * @param numRequests
     * @return a partial bulk request
     */
    public synchronized ConcurrentBulkRequest take(int numRequests) {
        Collection<ActionRequest> partRequest = Lists.newArrayListWithCapacity(numRequests);
        long size = 0L;
        for (int i = 0; i < numRequests; i++) {
            ActionRequest request = requests.poll();
            if (request != null) {
                // some nasty overhead to calculate the decreased byte size
                if (request instanceof IndexRequest) {
                    long l = ((IndexRequest) request).source().length() + REQUEST_OVERHEAD;
                    sizeInBytes.addAndGet(-l);
                    size += l;
                } else if (request instanceof DeleteRequest) {
                    sizeInBytes.addAndGet(-REQUEST_OVERHEAD);
                    size += REQUEST_OVERHEAD;
                }
                partRequest.add(request);
            }
        }
        return new ConcurrentBulkRequest().requests(partRequest, size);
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (ActionRequest request : requests) {
            ActionRequestValidationException ex = request.validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        // begin of copy from TransportRequest
        if (in.readBoolean()) {
            Map<String, Object> headers = in.readMap();
            for (String key : headers.keySet()) {
                putHeader(key, headers.get(key));
            }
        }
        // end of copy from TransportRequest
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            byte type = in.readByte();
            if (type == 0) {
                IndexRequest request = new IndexRequest();
                request.readFrom(in);
                requests.add(request);
            } else if (type == 1) {
                DeleteRequest request = new DeleteRequest();
                request.readFrom(in);
                requests.add(request);
            }
        }
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // hack - copied from TransportRequest
        if (getHeaders() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(getHeaders());
        }
        // end of copy from TransportRequest
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        out.writeVInt(requests.size());
        for (ActionRequest request : requests) {
            if (request instanceof IndexRequest) {
                out.writeByte((byte) 0);
            } else if (request instanceof DeleteRequest) {
                out.writeByte((byte) 1);
            }
            request.writeTo(out);
        }
        out.writeBoolean(refresh);
    }
}

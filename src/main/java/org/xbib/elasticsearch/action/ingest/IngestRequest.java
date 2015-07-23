package org.xbib.elasticsearch.action.ingest;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IngestRequest extends ActionRequest<IngestRequest> implements CompositeIndicesRequest {

    private static final int REQUEST_OVERHEAD = 50;

    private final Queue<ActionRequest<?>> requests = newQueue();

    private final AtomicLong sizeInBytes = new AtomicLong();

    private TimeValue timeout = Consistency.DEFAULT_TIMEOUT;

    private Consistency requiredConsistency = Consistency.DEFAULT_CONSISTENCY;

    private long ingestId;

    public IngestRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    public IngestRequest requiredConsistency(Consistency requiredConsistency) {
        this.requiredConsistency = requiredConsistency;
        return this;
    }

    public Consistency requiredConsistency() {
        return requiredConsistency;
    }

    public IngestRequest ingestId(long ingestId) {
        this.ingestId = ingestId;
        return this;
    }

    public long ingestId() {
        return ingestId;
    }

    public Queue<ActionRequest<?>> newQueue() {
        return new ConcurrentLinkedQueue<ActionRequest<?>>();
    }

    protected Queue<ActionRequest<?>> requests() {
        return requests;
    }

    public IngestRequest add(ActionRequest<?>... requests) {
        for (ActionRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    public IngestRequest add(ActionRequest<?> request) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request);
        } else {
            throw new IllegalArgumentException("no support for request [" + request + "]");
        }
        return this;
    }

    public IngestRequest add(Iterable<ActionRequest<?>> requests) {
        for (ActionRequest<?> request : requests) {
            if (request instanceof IndexRequest) {
                add((IndexRequest) request);
            } else if (request instanceof DeleteRequest) {
                add((DeleteRequest) request);
            } else {
                throw new IllegalArgumentException("no support for request [" + request + "]");
            }
        }
        return this;
    }

    public IngestRequest add(IndexRequest request) {
        return internalAdd(request);
    }

    public IngestRequest add(DeleteRequest request) {
        requests.offer(request);
        sizeInBytes.addAndGet(REQUEST_OVERHEAD);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<? extends IndicesRequest> subRequests() {
        List<IndicesRequest> indicesRequests = Lists.newArrayList();
        for (ActionRequest<?> request : requests) {
            assert request instanceof IndicesRequest;
            indicesRequests.add((IndicesRequest) request);
        }
        return indicesRequests;
    }

    /**
     * The number of actions in the ingest request.
     *
     * @return the number of actions
     */
    public int numberOfActions() {
        // for ConcurrentLinkedList, this call is not O(n), and may not be the size of the current list
        return requests.size();
    }

    /**
     * The estimated size in bytes of the ingest request.
     *
     * @return the estimated byte size
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes.longValue();
    }

    /**
     * Adds a framed data in binary format
     *
     * @param data   data
     * @param from   from
     * @param length length
     * @return this request
     * @throws Exception if data could not be added
     */
    public IngestRequest add(byte[] data, int from, int length) throws Exception {
        return add(data, from, length, null, null);
    }

    /**
     * Adds a framed data in binary format
     *
     * @param data         data
     * @param from         from
     * @param length       length
     * @param defaultIndex the default index
     * @param defaultType  the default type
     * @return this request
     * @throws Exception if data could not be added
     */
    public IngestRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(new BytesArray(data, from, length), defaultIndex, defaultType);
    }

    /**
     * Adds a framed data in binary format
     *
     * @param data         data
     * @param defaultIndex the default index
     * @param defaultType  the default type
     * @return this request
     * @throws Exception if data could not be added
     */
    public IngestRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
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
                                ttl = TimeValue.parseTimeValue(parser.text(), null, currentFieldName).millis();
                            } else {
                                ttl = parser.longValue();
                            }
                        } else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
                            opType = parser.text();
                        } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                            version = parser.longValue();
                        } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                            versionType = VersionType.fromString(parser.text());
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
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .source(data.slice(from, nextMarker - from)));
                        } else {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .create("create".equals(opType))
                                    .source(data.slice(from, nextMarker - from)));
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                .create(true)
                                .source(data.slice(from, nextMarker - from)));
                    }
                    from = nextMarker + 1;
                }
            } finally {
                parser.close();
            }
        }
        return this;
    }

    /**
     * Take all requests from queue. This method is thread safe.
     *
     * @return a bulk request
     */
    public IngestRequest takeAll() {
        IngestRequest request = new IngestRequest();
        while (!requests.isEmpty()) {
            ActionRequest<?> actionRequest = requests.poll();
            request.add(actionRequest);
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                long length = indexRequest.source() != null ? indexRequest.source().length() + REQUEST_OVERHEAD : REQUEST_OVERHEAD;
                sizeInBytes.addAndGet(-length);
            } else if (actionRequest instanceof DeleteRequest) {
                sizeInBytes.addAndGet(REQUEST_OVERHEAD);
            }
        }
        return request;
    }

    /**
     * Take a number of requests from the bulk request queue.
     * This method is thread safe.
     *
     * @param numRequests number of requests
     * @return a partial bulk request
     */
    public IngestRequest take(int numRequests) {
        IngestRequest request = new IngestRequest();
        for (int i = 0; i < numRequests; i++) {
            ActionRequest<?> actionRequest = requests.poll();
            request.add(actionRequest);
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                long length = indexRequest.source() != null ? indexRequest.source().length() + REQUEST_OVERHEAD : REQUEST_OVERHEAD;
                sizeInBytes.addAndGet(-length);
            } else if (actionRequest instanceof DeleteRequest) {
                sizeInBytes.addAndGet(REQUEST_OVERHEAD);
            } else {
                throw new IllegalStateException("action request not supported: " + actionRequest.getClass().getName());
            }
        }
        return request;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", null);
        }
        for (ActionRequest<?> request : requests) {
            if (request == null) {
                validationException = addValidationError("null request added", null);
            } else {
                ActionRequestValidationException ex = request.validate();
                if (ex != null) {
                    if (validationException == null) {
                        validationException = new ActionRequestValidationException();
                    }
                    validationException.addValidationErrors(ex.validationErrors());
                }
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timeout = TimeValue.readTimeValue(in);
        ingestId = in.readLong();
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timeout.writeTo(out);
        out.writeLong(ingestId);
        out.writeVInt(requests.size());
        for (ActionRequest<?> request : requests) {
            if (request instanceof IndexRequest) {
                out.writeByte((byte) 0);
            } else if (request instanceof DeleteRequest) {
                out.writeByte((byte) 1);
            }
            request.writeTo(out);
        }
    }

    IngestRequest internalAdd(IndexRequest request) {
        if (request == null) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("request must not be null");
            throw e;
        }
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            throw validationException;
        }
        requests.offer(request);
        sizeInBytes.addAndGet(request.source().length() + REQUEST_OVERHEAD);
        return this;
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }
}

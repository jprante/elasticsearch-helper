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

package org.xbib.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.List;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests. Each item holds the index/type/id is operated on, and if it failed or not (with the
 * failure message).
 */
public class IngestResponse extends ActionResponse {

    private List<IngestItemSuccess> success;
    private List<IngestItemFailure> failure;
    private long tookInMillis;

    IngestResponse() {
        this.success = Lists.newLinkedList();
        this.failure = Lists.newLinkedList();
    }

    public IngestResponse(List<IngestItemSuccess> success, List<IngestItemFailure> failure, long tookInMillis) {
        this.success = success;
        this.failure = failure;
        this.tookInMillis = tookInMillis;
    }

    public List<IngestItemSuccess> success() {
        return success;
    }

    public List<IngestItemFailure> failure() {
        return failure;
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue took() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue getTook() {
        return took();
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long tookInMillis() {
        return tookInMillis;
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis();
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        return !failure.isEmpty();
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (int i = 0; i < failure.size(); i++) {
            IngestItemFailure f = failure.get(i);
            sb.append("\n[").append(f.id()).append("], message [").append(f.message()).append("]");
        }
        return sb.toString();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        success = Lists.newLinkedList();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            success.add(new IngestItemSuccess(in.readVInt()));
        }
        failure = Lists.newLinkedList();
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            failure.add(new IngestItemFailure(in.readVInt(), in.readString()));
        }
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(success.size());
        for (IngestItemSuccess s : success) {
            out.writeVInt(s.id());
        }
        out.writeVInt(failure.size());
        for (IngestItemFailure f : failure) {
            out.writeVInt(f.id());
            out.writeString(f.message());
        }
        out.writeVLong(tookInMillis);
    }
}

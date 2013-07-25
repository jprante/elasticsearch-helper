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
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class IngestShardResponse implements ActionResponse {

    private ShardId shardId;
    private List<IngestItemSuccess> success;
    private List<IngestItemFailure> failure;

    IngestShardResponse() {
        this.success = Lists.newLinkedList();
        this.failure = Lists.newLinkedList();
    }

    IngestShardResponse(ShardId shardId, List<IngestItemSuccess> success,  List<IngestItemFailure> failure) {
        this.shardId = shardId;
        this.success = success;
        this.failure = failure;
    }

    public ShardId shardId() {
        return shardId;
    }

    public List<IngestItemSuccess> success() {
        return success;
    }

    public List<IngestItemFailure> failure() {
        return failure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        success = Lists.newLinkedList();
        for (int i = 0; i < in.readVInt(); i++) {
            success.add(new IngestItemSuccess(in.readVInt()));
        }
        failure = Lists.newLinkedList();
        for (int i = 0; i < in.readVInt(); i++) {
            failure.add(new IngestItemFailure(in.readVInt(), in.readString()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(success.size());
        for (IngestItemSuccess s : success) {
            out.writeVInt(s.id());
        }
        out.writeVInt(failure.size());
        for (IngestItemFailure f : failure) {
            out.writeVInt(f.id());
            out.writeString(f.message());
        }
    }
}

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

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class IngestShardRequest extends ShardReplicationOperationRequest<IngestShardRequest> {

    private int shardId;

    private List<IngestItemRequest> items;

    private boolean refresh;

    IngestShardRequest() {
    }

    IngestShardRequest(String index, int shardId, boolean refresh, List<IngestItemRequest> items) {
        this.index = index;
        this.shardId = shardId;
        this.items = items;
        this.refresh = refresh;
    }

    boolean refresh() {
        return this.refresh;
    }

    int shardId() {
        return shardId;
    }

    List<IngestItemRequest> items() {
        return items;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override
    public void beforeLocalFork() {
        for (IngestItemRequest item : items) {
            ((ShardReplicationOperationRequest) item.request()).beforeLocalFork();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
        out.writeVInt(items.size());
        for (IngestItemRequest item : items) {
            if (item != null) {
                out.writeBoolean(true);
                item.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
        out.writeBoolean(refresh);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
        int size = in.readVInt();
        items = new ArrayList();
        for (int i = 0; i < size; i++) {
            if (in.readBoolean()) {
                items.add(IngestItemRequest.readBulkItem(in));
            }
        }
        refresh = in.readBoolean();
    }
}

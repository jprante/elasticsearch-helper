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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestBulkIngestResponseAction extends BaseRestHandler {

    @Inject
    public RestBulkIngestResponseAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/_bulkingest", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        try {
            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            builder.field(Fields.OK, true);
            builder.startArray(Fields.RESPONSES);
            synchronized (RestBulkIngestAction.responses) {
                for (Map.Entry<Long, Object> me : RestBulkIngestAction.responses.entrySet()) {
                    builder.startObject();
                    builder.field(Fields.ID, me.getKey());
                    if (me.getValue() instanceof BulkResponse) {
                        BulkResponse response = (BulkResponse) me.getValue();
                        builder.startObject();
                        builder.field(Fields.TOOK, response.getTookInMillis());
                        builder.startArray(Fields.ITEMS);
                        for (BulkItemResponse itemResponse : response) {
                            builder.startObject();
                            builder.startObject(itemResponse.opType());
                            builder.field(Fields._INDEX, itemResponse.getIndex());
                            builder.field(Fields._TYPE, itemResponse.getType());
                            builder.field(Fields._ID, itemResponse.getId());
                            long version = itemResponse.version();
                            if (version != -1) {
                                builder.field(Fields._VERSION, itemResponse.version());
                            }
                            if (itemResponse.isFailed()) {
                                builder.field(Fields.ERROR, itemResponse.getFailure().getMessage());
                            } else {
                                builder.field(Fields.OK, true);
                            }
                            if (itemResponse.response() instanceof IndexResponse) {
                                IndexResponse indexResponse = itemResponse.response();
                                if (indexResponse.getMatches() != null) {
                                    builder.startArray(Fields.MATCHES);
                                    for (String match : indexResponse.getMatches()) {
                                        builder.value(match);
                                    }
                                    builder.endArray();
                                }
                            }
                            builder.endObject();
                            builder.endObject();
                        }
                        builder.endArray();
                        builder.endObject();
                    } else if (me.getValue() instanceof Throwable) {
                        Throwable t = (Throwable)me.getValue();
                        builder.field(Fields.ERROR, t.getMessage());
                    }
                    builder.endObject();
                }
                RestBulkIngestAction.responses.clear(); // write each response only once
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
        static final XContentBuilderString ITEMS = new XContentBuilderString("items");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }
}

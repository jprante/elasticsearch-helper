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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A concurrent bulk transport action
 * <p/>
 * This action registers a ConcurrentTransportHandler to the transport service
 */
public class TransportIngestAction extends TransportAction<IngestRequest, IngestResponse> {

    private final boolean autoCreateIndex;

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportShardIngestAction shardBulkAction;

    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportIngestAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                 TransportShardIngestAction shardBulkAction, TransportCreateIndexAction createIndexAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;

        this.autoCreateIndex = settings.getAsBoolean("action.auto_create_index", true);
        this.allowIdGeneration = componentSettings.getAsBoolean("action.allow_id_generation", true);

        transportService.registerHandler(IngestAction.NAME, new IngestTransportHandler());
    }

    @Override
    protected void doExecute(final IngestRequest ingestRequest, final ActionListener<IngestResponse> listener) {
        final long startTime = System.currentTimeMillis();
        Set<String> indices = Sets.newHashSet();
        for (ActionRequest request : ingestRequest.requests()) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                if (!indices.contains(indexRequest.index())) {
                    indices.add(indexRequest.index());
                }
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                if (!indices.contains(deleteRequest.index())) {
                    indices.add(deleteRequest.index());
                }
            }
        }

        if (autoCreateIndex) {
            final AtomicInteger counter = new AtomicInteger(indices.size());
            final AtomicBoolean failed = new AtomicBoolean();
            for (String index : indices) {
                if (!clusterService.state().metaData().hasConcreteIndex(index)) {
                    createIndexAction.execute(new CreateIndexRequest(index).cause("auto(bulk api)"), new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(ingestRequest, startTime, listener);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                                // we have the index, do it
                                if (counter.decrementAndGet() == 0) {
                                    executeBulk(ingestRequest, startTime, listener);
                                }
                            } else if (failed.compareAndSet(false, true)) {
                                listener.onFailure(e);
                            }
                        }
                    });
                } else {
                    if (counter.decrementAndGet() == 0) {
                        executeBulk(ingestRequest, startTime, listener);
                    }
                }
            }
        } else {
            executeBulk(ingestRequest, startTime, listener);
        }
    }

    private void executeBulk(final IngestRequest ingestRequest, final long startTime, final ActionListener<IngestResponse> listener) {
        ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        MetaData metaData = clusterState.metaData();
        for (ActionRequest request : ingestRequest.requests()) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                String aliasOrIndex = indexRequest.index();
                indexRequest.index(clusterState.metaData().concreteIndex(indexRequest.index()));

                MappingMetaData mappingMd = null;
                if (metaData.hasIndex(indexRequest.index())) {
                    mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
                }
                indexRequest.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                deleteRequest.routing(clusterState.metaData().resolveIndexRouting(deleteRequest.routing(), deleteRequest.index()));
                deleteRequest.index(clusterState.metaData().concreteIndex(deleteRequest.index()));
            }
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        Map<ShardId, List<IngestItemRequest>> requestsByShard = Maps.newHashMap();
        int i = 0;
        for (ActionRequest request : ingestRequest.requests()) {
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<IngestItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = Lists.newArrayList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new IngestItemRequest(i, request));
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                MappingMetaData mappingMd = clusterState.metaData().index(deleteRequest.index()).mappingOrDefault(deleteRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && deleteRequest.routing() == null) {
                    // if routing is required, and no routing on the delete request, we need to broadcast it....
                    GroupShardsIterator groupShards = clusterService.operationRouting().broadcastDeleteShards(clusterState, deleteRequest.index());
                    for (ShardIterator shardIt : groupShards) {
                        List<IngestItemRequest> list = requestsByShard.get(shardIt.shardId());
                        if (list == null) {
                            list = Lists.newArrayList();
                            requestsByShard.put(shardIt.shardId(), list);
                        }
                        list.add(new IngestItemRequest(i, new DeleteRequest(deleteRequest)));
                    }
                } else {
                    ShardId shardId = clusterService.operationRouting().deleteShards(clusterState, deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                    List<IngestItemRequest> list = requestsByShard.get(shardId);
                    if (list == null) {
                        list = Lists.newArrayList();
                        requestsByShard.put(shardId, list);
                    }
                    list.add(new IngestItemRequest(i, request));
                }
            }
            i++;
        }

        final List<IngestItemSuccess> success = Lists.newLinkedList();
        final List<IngestItemFailure> failure = Lists.newLinkedList();

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new IngestResponse(success, failure, System.currentTimeMillis() - startTime));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<IngestItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<IngestItemRequest> requests = entry.getValue();
            IngestShardRequest ingestShardRequest = new IngestShardRequest(shardId.index().name(), shardId.id(), ingestRequest.refresh(), requests);
            ingestShardRequest.replicationType(ingestRequest.replicationType());
            ingestShardRequest.consistencyLevel(ingestRequest.consistencyLevel());
            shardBulkAction.execute(ingestShardRequest, new ActionListener<IngestShardResponse>() {
                @Override
                public void onResponse(IngestShardResponse ingestShardResponse) {
                    synchronized (success) {
                        success.addAll(ingestShardResponse.success());
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // create failures for all relevant requests
                    String message = ExceptionsHelper.detailedMessage(e);
                    synchronized (failure) {
                        for (IngestItemRequest request : requests) {
                            failure.add(new IngestItemFailure(request.id(), message));
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new IngestResponse(success, failure, System.currentTimeMillis() - startTime));
                }
            });
        }
    }

    class IngestTransportHandler extends BaseTransportRequestHandler<IngestRequest> {

        @Override
        public IngestRequest newInstance() {
            return new IngestRequest();
        }

        @Override
        public void messageReceived(final IngestRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<IngestResponse>() {
                @Override
                public void onResponse(IngestResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [" + IngestAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}

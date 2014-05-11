package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.support.river.RiverHelper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportRiverStateAction extends TransportNodesOperationAction<RiverStateRequest, RiverStateResponse, NodeRiverStateRequest, NodeRiverStateResponse> {

    private final NodeService nodeService;

    private final Injector injector;

    @Inject
    public TransportRiverStateAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                     ClusterService clusterService, TransportService transportService,
                                     NodeService nodeService, Injector injector) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.nodeService = nodeService;
        this.injector = injector;
    }

    @Override
    protected String transportAction() {
        return RiverStateAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodeRiverStateResponse nodeOperation(NodeRiverStateRequest request) throws ElasticsearchException {
        try {
            NodeInfo nodeInfo = nodeService.info(false, true, false, true, false, false, true, false, true);
            String riverName = request.getRiverName();
            String riverType = request.getRiverType();
            NodeRiverStateResponse nodeRiverStateResponse = new NodeRiverStateResponse(nodeInfo.getNode());
            for (Map.Entry<RiverName, River> entry : RiverHelper.rivers(injector).entrySet()) {
                RiverName name = entry.getKey();
                if ((riverName == null || "*".equals(riverName) || name.getName().equals(riverName))
                        && (riverType == null || "*".equals(riverType) || name.getType().equals(riverType))
                        && entry.getValue() instanceof StatefulRiver) {
                    StatefulRiver river = (StatefulRiver) entry.getValue();
                    nodeRiverStateResponse.addState(river.getRiverState());
                }
            }
            return nodeRiverStateResponse;
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        throw new ElasticsearchException("error in node operation");
    }

    @Override
    protected RiverStateRequest newRequest() {
        return new RiverStateRequest();
    }

    @Override
    protected RiverStateResponse newResponse(RiverStateRequest request, AtomicReferenceArray nodeResponses) {
        RiverStateResponse riverStateResponse = new RiverStateResponse();
        for (int i = 0; i < nodeResponses.length(); i++) {
            Object nodeResponse = nodeResponses.get(i);
            if (nodeResponse instanceof NodeRiverStateResponse) {
                NodeRiverStateResponse nodeRiverStateResponse = (NodeRiverStateResponse) nodeResponse;
                if (nodeRiverStateResponse.getStates() != null) {
                    for (RiverState riverState : nodeRiverStateResponse.getStates()) {
                        riverStateResponse.addState(riverState);
                    }
                }
            }
        }
        return riverStateResponse;
    }

    @Override
    protected NodeRiverStateRequest newNodeRequest() {
        return new NodeRiverStateRequest();
    }

    @Override
    protected NodeRiverStateRequest newNodeRequest(String nodeId, RiverStateRequest request) {
        return new NodeRiverStateRequest(nodeId, request);
    }

    @Override
    protected NodeRiverStateResponse newNodeResponse() {
        return new NodeRiverStateResponse();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

}

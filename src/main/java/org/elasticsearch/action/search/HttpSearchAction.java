
package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.xbib.elasticsearch.helper.client.http.HttpAction;
import org.xbib.elasticsearch.helper.client.http.HttpContext;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    public HttpSearchAction(Settings settings) {
        super(settings, SearchAction.NAME);
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, SearchRequest request) throws IOException {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newRequest(HttpMethod.POST, url, index + "/_search", request.extraSource());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SearchResponse createResponse(HttpContext<SearchRequest,SearchResponse> httpContext) throws IOException {
        if (httpContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpContext.getHttpResponse();
        logger.info("{}", httpResponse.getContent().toString(CharsetUtil.UTF_8));
        BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
        Map<String,Object> map = JsonXContent.jsonXContent.createParser(ref).map();

        logger.info("{}", map);

        InternalSearchResponse internalSearchResponse = parseInternalSearchResponse(map);
        String scrollId = (String)map.get(SCROLL_ID);
        int totalShards = 0;
        int successfulShards = 0;
        if (map.containsKey(SHARDS)) {
            Map<String,?> shards = (Map<String,?>)map.get(SHARDS);
            totalShards =  shards.containsKey(TOTAL) ? (Integer)shards.get(TOTAL) : -1;
            successfulShards =  shards.containsKey(SUCCESSFUL) ? (Integer)shards.get(SUCCESSFUL) : -1;
        }
        int tookInMillis = map.containsKey(TOOK) ? (Integer)map.get(TOOK) : -1;
        ShardSearchFailure[] shardFailures = parseShardFailures(map);
        return new SearchResponse(internalSearchResponse, scrollId, totalShards, successfulShards, tookInMillis, shardFailures);
    }

    private InternalSearchResponse parseInternalSearchResponse(Map<String,?> map) {
        InternalSearchHits internalSearchHits = parseInternalSearchHits(map);
        InternalAggregations internalAggregations = parseInternalAggregations(map);
        Suggest suggest = parseSuggest(map);
        Boolean timeout = false;
        Boolean terminatedEarly = false;
        return new InternalSearchResponse(internalSearchHits, internalAggregations, suggest, timeout, terminatedEarly);
    }

    @SuppressWarnings("unchecked")
    private InternalSearchHits parseInternalSearchHits(Map<String,?> map) {
        // InternalSearchHits(InternalSearchHit[] hits, long totalHits, float maxScore)
        InternalSearchHit[] internalSearchHits = parseInternalSearchHit(map);
        map = (Map<String, ?>) map.get(HITS);
        long totalHits = map.containsKey(TOTAL) ? (Integer)map.get(TOTAL) : -1L;
        double maxScore = map.containsKey(MAXSCORE) ? (Double)map.get(MAXSCORE) : 0.0f;
        return new InternalSearchHits(internalSearchHits, totalHits, (float)maxScore);
    }

    @SuppressWarnings("unchecked")
    private InternalSearchHit[] parseInternalSearchHit(Map<String,?> map) {
        // public InternalSearchHit(int docId, String id, Text type, Map<String, SearchHitField> fields)
        List<InternalSearchHit> list = new LinkedList<>();
        Map<String,?> hitmap = (Map<String, ?>) map.get(HITS);
        List<Map<String,?>> hits = (List<Map<String, ?>>) hitmap.get(HITS);
        return list.toArray(new InternalSearchHit[list.size()]);
    }

    private InternalAggregations parseInternalAggregations(Map<String,?> map) {
        return null;
    }

    private Suggest parseSuggest(Map<String,?> map) {
        return null;
    }

    private ShardSearchFailure[] parseShardFailures(Map<String,?> map) {
        return ShardSearchFailure.EMPTY_ARRAY;
    }

    private final static String SCROLL_ID = "_scroll_id";
    private final static String TOOK = "took";
    private final static String TIMED_OUT = "timed_out";

    private final static String SHARDS = "_shards";
    private final static String TOTAL = "total";
    private final static String SUCCESSFUL = "successful";
    private final static String FAILED = "failed";
    private final static String FAILURES = "failures";

    private final static String HITS = "hits";
    private final static String MAXSCORE = "max_score";
}


package org.xbib.elasticsearch.action.search.helper;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Helper class for Elasticsearch search requests
 */
public class BasicSearchRequest {

    private final ESLogger logger = ESLoggerFactory.getLogger(BasicSearchRequest.class.getName());

    private SearchRequestBuilder searchRequestBuilder;

    private String[] index;

    private String[] type;

    private String id;

    private String query;

    public BasicSearchRequest newRequest(SearchRequestBuilder searchRequestBuilder) {
        this.searchRequestBuilder = searchRequestBuilder;
        return this;
    }

    public SearchRequestBuilder searchRequestBuilder() {
        return searchRequestBuilder;
    }

    public BasicSearchRequest index(String index) {
        if (index != null && !"*".equals(index)) {
            this.index = new String[]{index};
        }
        return this;
    }

    public BasicSearchRequest index(String... index) {
        this.index = index;
        return this;
    }

    public String index() {
        return index[0];
    }

    public BasicSearchRequest type(String type) {
        if (type != null && !"*".equals(type)) {
            this.type = new String[]{type};
        }
        return this;
    }

    public BasicSearchRequest type(String... type) {
        this.type = type;
        return this;
    }

    public String type() {
        return type[0];
    }

    public BasicSearchRequest id(String id) {
        this.id = id;
        return this;
    }

    public String id() {
        return id;
    }

    public BasicSearchRequest from(int from) {
        searchRequestBuilder.setFrom(from);
        return this;
    }

    public BasicSearchRequest size(int size) {
        searchRequestBuilder.setSize(size);
        return this;
    }

    public BasicSearchRequest postFilter(String filter) {
        searchRequestBuilder.setPostFilter(filter);
        return this;
    }

    public BasicSearchRequest aggregations(String aggregations) {
        searchRequestBuilder.setAggregations(aggregations.getBytes());
        return this;
    }

    public BasicSearchRequest timeout(TimeValue timeout) {
        searchRequestBuilder.setTimeout(timeout);
        return this;
    }

    public BasicSearchRequest query(String query) {
        this.query = query == null || query.trim().length() == 0 ? "{\"query\":{\"match_all\":{}}}" : query;
        return this;
    }

    public BasicSearchResponse execute()
            throws IOException {
        BasicSearchResponse response = new BasicSearchResponse();
        if (searchRequestBuilder == null) {
            return response;
        }
        if (query == null) {
            return response;
        }
        if (hasIndex(index)) {
            searchRequestBuilder.setIndices(fixIndexName(index));
        }
        if (hasType(type)) {
            searchRequestBuilder.setTypes(type);
        }
        long t0 = System.currentTimeMillis();
        response.setResponse(searchRequestBuilder.setExtraSource(query).execute().actionGet());
        long t1 = System.currentTimeMillis();
        logger.info(" [{}] [{}ms] [{}ms] [{}] [{}]",
                formatIndexType(), t1 - t0, response.tookInMillis(), response.totalHits(), query);
        return response;
    }

    private boolean hasIndex(String[] s) {
        return s != null && s.length != 0 && s[0] != null;
    }

    private boolean hasType(String[] s) {
        return s != null && s.length != 0 && s[0] != null;
    }

    private String[] fixIndexName(String[] s) {
        if (s == null) {
            return new String[]{"*"};
        }
        if (s.length == 0) {
            return new String[]{"*"};
        }
        for (int i = 0; i < s.length; i++) {
            if (s[i] == null || s[i].length() == 0) {
                s[i] = "*";
            }
        }
        return s;
    }

    private String formatIndexType() {
        StringBuilder indexes = new StringBuilder();
        if (index != null) {
            for (String s : index) {
                if (s != null && s.length() > 0) {
                    if (indexes.length() > 0) {
                        indexes.append(',');
                    }
                    indexes.append(s);
                }
            }
        }
        if (indexes.length() == 0) {
            indexes.append('*');
        }
        StringBuilder types = new StringBuilder();
        if (type != null) {
            for (String s : type) {
                if (s != null && s.length() > 0) {
                    if (types.length() > 0) {
                        types.append(',');
                    }
                    types.append(s);
                }
            }
        }
        if (types.length() == 0) {
            types.append('*');
        }
        return indexes.append("/").append(types).toString();
    }

}

package org.xbib.elasticsearch.action.search.helper;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;

/**
 * Helper class for Elasticsearch get requests
 */
public class BasicGetRequest {

    private final ESLogger logger = ESLoggerFactory.getLogger(BasicGetRequest.class.getName());

    private GetRequestBuilder getRequestBuilder;

    private String index;

    private String type;

    private String id;

    public BasicGetRequest newRequest(GetRequestBuilder getRequestBuilder) {
        this.getRequestBuilder = getRequestBuilder;
        return this;
    }

    public GetRequestBuilder getRequestBuilder() {
        return getRequestBuilder;
    }

    public BasicGetRequest index(String index) {
        if (index != null && !"*".equals(index)) {
            this.index = index;
        }
        return this;
    }

    public String index() {
        return index;
    }

    public BasicGetRequest type(String type) {
        if (type != null && !"*".equals(type)) {
            this.type = type;
        }
        return this;
    }

    public String type() {
        return type;
    }

    public BasicGetRequest id(String id) {
        this.id = id;
        return this;
    }

    public String id() {
        return id;
    }

    public BasicGetResponse execute() throws IOException {
        BasicGetResponse response = new BasicGetResponse();
        if (getRequestBuilder == null) {
            return response;
        }
        logger.info(" get request: {}/{}/{}",
                getRequestBuilder.request().index(),
                getRequestBuilder.request().type(),
                getRequestBuilder.request().id());
        getRequestBuilder
                .setIndex(index)
                .setType(type)
                .setId(id);
        long t0 = System.currentTimeMillis();
        response.setResponse(getRequestBuilder.execute().actionGet());
        long t1 = System.currentTimeMillis();
        logger.info(" get request complete: {}/{}/{} [{}ms] {}",
                getRequestBuilder.request().index(),
                getRequestBuilder.request().type(),
                getRequestBuilder.request().id(),
                (t1 - t0), response.exists());
        return response;
    }

}

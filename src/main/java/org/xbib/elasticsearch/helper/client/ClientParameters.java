package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public interface ClientParameters {

    int DEFAULT_MAX_ACTIONS_PER_REQUEST = 1000;

    int DEFAULT_MAX_CONCURRENT_REQUESTS = Runtime.getRuntime().availableProcessors() * 4;

    ByteSizeValue DEFAULT_MAX_VOLUME_PER_REQUEST = new ByteSizeValue(10, ByteSizeUnit.MB);

    TimeValue DEFAULT_FLUSH_INTERVAL = TimeValue.timeValueSeconds(30);

    String MAX_ACTIONS_PER_REQUEST = "max_actions_per_request";

    String MAX_CONCURRENT_REQUESTS = "max_concurrent_requests";

    String MAX_VOLUME_PER_REQUEST = "max_volume_per_request";

    String FLUSH_INTERVAL = "flush_interval";

}

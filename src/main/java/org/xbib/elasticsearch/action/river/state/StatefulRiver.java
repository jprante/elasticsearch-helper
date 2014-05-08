package org.xbib.elasticsearch.action.river.state;

import org.elasticsearch.river.River;

public interface StatefulRiver extends River {

    RiverState getRiverState();
}

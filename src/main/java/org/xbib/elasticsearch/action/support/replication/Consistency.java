package org.xbib.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

public enum Consistency {

    IGNORE((byte) 0),

    ONE((byte) 1),

    QUORUM((byte) 2),

    ALL((byte) 3);

    private final byte id;

    Consistency(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static Consistency fromId(byte value) {
        if (value == 0) {
            return IGNORE;
        } else if (value == 1) {
            return ONE;
        } else if (value == 2) {
            return QUORUM;
        } else if (value == 3) {
            return ALL;
        }
        throw new ElasticsearchIllegalArgumentException("No consistency match [" + value + "]");
    }

    public static Consistency fromString(String value) {
        if (value.equals("ignore")) {
            return IGNORE;
        } else if (value.equals("one")) {
            return ONE;
        } else if (value.equals("quorum")) {
            return QUORUM;
        } else if (value.equals("all")) {
            return ALL;
        }
        throw new ElasticsearchIllegalArgumentException("No consistency match [" + value + "]");
    }
}

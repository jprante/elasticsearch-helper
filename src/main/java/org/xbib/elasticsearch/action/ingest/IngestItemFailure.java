
package org.xbib.elasticsearch.action.ingest;

public class IngestItemFailure {

    private int pos;

    private String message;

    public IngestItemFailure(int pos, String message) {
        this.pos = pos;
        this.message = message;
    }

    public int pos() {
        return pos;
    }

    public String message() {
        return message;
    }
}

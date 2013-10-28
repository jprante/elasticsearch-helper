
package org.xbib.elasticsearch.action.ingest;

public class IngestItemFailure {

    private int id;

    private String message;

    public IngestItemFailure(int id, String message) {
        this.id = id;
        this.message = message;
    }

    public int id() {
        return id;
    }

    public String message() {
        return message;
    }
}

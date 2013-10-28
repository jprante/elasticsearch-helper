
package org.xbib.elasticsearch.action.ingest;

public class IngestItemSuccess {

    private int id;

    public IngestItemSuccess(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}


package org.xbib.elasticsearch.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.action.ingest.RestIngestAction;
import org.xbib.elasticsearch.action.ingest.RestIngestResponseAction;

/**
 * Ingest plugin. A replacement for bulk. Returns only short bulk responses
 * and allows concurrenct in the action bulk request building.
 * Must be installed on server and client.
 */
public class IngestPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "ingest";
    }

    @Override
    public String description() {
        return "Ingest action plugin";
    }

    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
        module.addRestAction(RestIngestResponseAction.class);
    }

}

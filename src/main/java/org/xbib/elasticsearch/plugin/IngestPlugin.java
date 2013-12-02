
package org.xbib.elasticsearch.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.action.ingest.RestIngestAction;
import org.xbib.elasticsearch.action.ingest.create.IngestCreateAction;
import org.xbib.elasticsearch.action.ingest.create.TransportIngestCreateAction;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteAction;
import org.xbib.elasticsearch.action.ingest.delete.TransportIngestDeleteAction;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexAction;
import org.xbib.elasticsearch.action.ingest.index.TransportIngestIndexAction;

/**
 * Ingest plugin
 *
 * A replacement for bulk action. Returns only short bulk responses
 * and allows multi threaded concurrent access in the action bulk request building.
 */
public class IngestPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "ingest";
    }

    @Override
    public String description() {
        return "Ingest plugin";
    }

    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);

        module.registerAction(IngestCreateAction.INSTANCE, TransportIngestCreateAction.class);
        module.registerAction(IngestDeleteAction.INSTANCE, TransportIngestDeleteAction.class);
        module.registerAction(IngestIndexAction.INSTANCE, TransportIngestIndexAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
    }

}

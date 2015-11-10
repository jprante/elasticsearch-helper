package org.xbib.elasticsearch.plugin.helper;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.rest.action.ingest.RestIngestAction;

/**
 * Helper plugin
 */
public class HelperPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "helper";
    }

    @Override
    public String description() {
        return "Helper plugin";
    }



    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
    }

}

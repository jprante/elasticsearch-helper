package org.xbib.elasticsearch.plugin.support;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.action.river.execute.RiverExecuteAction;
import org.xbib.elasticsearch.action.river.execute.TransportRiverExecuteAction;
import org.xbib.elasticsearch.action.river.state.RiverStateAction;
import org.xbib.elasticsearch.action.river.state.TransportRiverStateAction;
import org.xbib.elasticsearch.rest.action.ingest.RestIngestAction;
import org.xbib.elasticsearch.rest.action.river.execute.RestRiverExecuteAction;
import org.xbib.elasticsearch.rest.action.river.state.RestRiverStateAction;

/**
 * Support plugin
 */
public class SupportPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "support-"
                + Build.getInstance().getVersion() + "-"
                + Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "Support plugin";
    }

    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);
        module.registerAction(RiverExecuteAction.INSTANCE, TransportRiverExecuteAction.class);
        module.registerAction(RiverStateAction.INSTANCE, TransportRiverStateAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
        module.addRestAction(RestRiverExecuteAction.class);
        module.addRestAction(RestRiverStateAction.class);
    }

}

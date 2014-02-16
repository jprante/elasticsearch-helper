
package org.xbib.elasticsearch.plugin.support;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.rest.action.ingest.RestIngestAction;
import org.xbib.elasticsearch.action.ingest.delete.IngestDeleteAction;
import org.xbib.elasticsearch.action.ingest.delete.TransportIngestDeleteAction;
import org.xbib.elasticsearch.action.ingest.index.IngestIndexAction;
import org.xbib.elasticsearch.action.ingest.index.TransportIngestIndexAction;

/**
 * Support plugin
 */
public class SupportPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "support-" +
                Build.getInstance().getVersion() + "-" +
                Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "Support plugin";
    }

    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);
        module.registerAction(IngestDeleteAction.INSTANCE, TransportIngestDeleteAction.class);
        module.registerAction(IngestIndexAction.INSTANCE, TransportIngestIndexAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
    }

}

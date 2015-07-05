package org.xbib.elasticsearch.plugin.support;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.ingest.IngestAction;
import org.xbib.elasticsearch.action.ingest.TransportIngestAction;
import org.xbib.elasticsearch.rest.action.ingest.RestIngestAction;

/**
 * Support plugin
 */
public class SupportPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "support-"
                + Build.getInstance().getVersion()
                + "-" + Build.getInstance().getShortHash()
                + " " + System.getProperty("os.name")
                + " " + System.getProperty("java.vm.name")
                + " " + System.getProperty("java.vm.vendor")
                + " " + System.getProperty("java.runtime.version")
                + " " + System.getProperty("java.vm.version");
    }

    @Override
    public String description() {
        return "Support plugin";
    }


    public void onModule(ActionModule module) {
        module.registerAction(IngestAction.INSTANCE, TransportIngestAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestIngestAction.class);
    }

}

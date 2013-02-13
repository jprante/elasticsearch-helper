package org.elasticsearch.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.bulk.ConcurrentBulkAction;
import org.elasticsearch.action.bulk.ConcurrentTransportBulkAction;
import org.elasticsearch.plugins.AbstractPlugin;

public class ConcurrentBulkPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "concurrent-bulk";
    }

    @Override
    public String description() {
        return "A concurrent bulk action";
    }

    public void onModule(ActionModule module) {
        module.registerAction(ConcurrentBulkAction.INSTANCE, ConcurrentTransportBulkAction.class);
    }

}

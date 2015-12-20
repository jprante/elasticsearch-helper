
package org.xbib.elasticsearch.helper.client.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.HttpClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.refresh.HttpRefreshIndexAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.settings.put.HttpUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.search.HttpSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.action.admin.indices.create.HttpCreateIndexAction;
import org.elasticsearch.action.bulk.HttpBulkAction;

import java.util.List;
import java.util.Map;

public class HttpActionModule extends AbstractModule {

    private final Map<String, ActionEntry> actions = Maps.newHashMap();
    private final List<Class<? extends ActionFilter>> actionFilters = Lists.newArrayList();

    static class ActionEntry<Request extends ActionRequest, Response extends ActionResponse> {
        public final GenericAction<Request, Response> action;
        public final Class<? extends HttpAction<Request, Response>> httpAction;

        ActionEntry(GenericAction<Request, Response> action, Class<? extends HttpAction<Request, Response>> httpAction) {
            this.action = action;
            this.httpAction = httpAction;
        }
    }

    public HttpActionModule() {
    }

    /**
     * Registers an action.
     *
     * @param action                  The action type.
     * @param httpAction         The http action implementing the actual action.
     * @param <Request>               The request type.
     * @param <Response>              The response type.
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void registerAction(GenericAction<Request, Response> action,
                                                                                                Class<? extends HttpAction<Request, Response>> httpAction) {
        actions.put(action.name(), new ActionEntry<>(action, httpAction));
    }

    public HttpActionModule registerFilter(Class<? extends ActionFilter> actionFilter) {
        actionFilters.add(actionFilter);
        return this;
    }

    @Override
    protected void configure() {

        /*Multibinder<ActionFilter> actionFilterMultibinder = Multibinder.newSetBinder(binder(), ActionFilter.class);
        for (Class<? extends ActionFilter> actionFilter : actionFilters) {
            actionFilterMultibinder.addBinding().to(actionFilter);
        }
        bind(ActionFilters.class).asEagerSingleton();
        bind(AutoCreateIndex.class).asEagerSingleton();*/

        registerAction(BulkAction.INSTANCE, HttpBulkAction.class);

        /*registerAction(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        registerAction(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        registerAction(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);

        registerAction(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        registerAction(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        registerAction(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);*/
        registerAction(ClusterUpdateSettingsAction.INSTANCE, HttpClusterUpdateSettingsAction.class);
        /*
        registerAction(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        registerAction(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        registerAction(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        registerAction(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        registerAction(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        registerAction(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        registerAction(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        registerAction(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        registerAction(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        registerAction(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        registerAction(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        registerAction(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        registerAction(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        registerAction(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        registerAction(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);*/
        registerAction(CreateIndexAction.INSTANCE, HttpCreateIndexAction.class);
        /*registerAction(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        registerAction(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        registerAction(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        registerAction(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        registerAction(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        registerAction(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        registerAction(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        registerAction(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class, TransportGetFieldMappingsIndexAction.class);
        registerAction(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        registerAction(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);*/
        registerAction(UpdateSettingsAction.INSTANCE, HttpUpdateSettingsAction.class);
        /*registerAction(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        registerAction(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        registerAction(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        registerAction(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        registerAction(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);*/
        registerAction(RefreshAction.INSTANCE, HttpRefreshIndexAction.class);
        /*registerAction(FlushAction.INSTANCE, TransportFlushAction.class);
        registerAction(OptimizeAction.INSTANCE, TransportOptimizeAction.class);
        registerAction(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        registerAction(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        registerAction(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        registerAction(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        registerAction(PutWarmerAction.INSTANCE, TransportPutWarmerAction.class);
        registerAction(DeleteWarmerAction.INSTANCE, TransportDeleteWarmerAction.class);
        registerAction(GetWarmersAction.INSTANCE, TransportGetWarmersAction.class);
        registerAction(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        registerAction(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);
        registerAction(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        registerAction(IndexAction.INSTANCE, TransportIndexAction.class);
        registerAction(GetAction.INSTANCE, TransportGetAction.class);
        registerAction(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class,
                TransportDfsOnlyAction.class);
        registerAction(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class,
                TransportShardMultiTermsVectorAction.class);
        registerAction(DeleteAction.INSTANCE, TransportDeleteAction.class);
        registerAction(ExistsAction.INSTANCE, TransportExistsAction.class);
        registerAction(SuggestAction.INSTANCE, TransportSuggestAction.class);
        registerAction(UpdateAction.INSTANCE, TransportUpdateAction.class);
        registerAction(MultiGetAction.INSTANCE, TransportMultiGetAction.class,
                TransportShardMultiGetAction.class);
        registerAction(BulkAction.INSTANCE, TransportBulkAction.class,
                TransportShardBulkAction.class);*/
        registerAction(SearchAction.INSTANCE, HttpSearchAction.class);
        /*registerAction(SearchAction.INSTANCE, TransportSearchAction.class,
                TransportSearchDfsQueryThenFetchAction.class,
                TransportSearchQueryThenFetchAction.class,
                TransportSearchDfsQueryAndFetchAction.class,
                TransportSearchQueryAndFetchAction.class,
                TransportSearchScanAction.class
        );
        registerAction(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class,
                TransportSearchScrollScanAction.class,
                TransportSearchScrollQueryThenFetchAction.class,
                TransportSearchScrollQueryAndFetchAction.class
        );
        registerAction(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        registerAction(PercolateAction.INSTANCE, TransportPercolateAction.class);
        registerAction(MultiPercolateAction.INSTANCE, TransportMultiPercolateAction.class, TransportShardMultiPercolateAction.class);
        registerAction(ExplainAction.INSTANCE, TransportExplainAction.class);
        registerAction(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        registerAction(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        registerAction(RenderSearchTemplateAction.INSTANCE, TransportRenderSearchTemplateAction.class);

        //Indexed scripts
        registerAction(PutIndexedScriptAction.INSTANCE, TransportPutIndexedScriptAction.class);
        registerAction(GetIndexedScriptAction.INSTANCE, TransportGetIndexedScriptAction.class);
        registerAction(DeleteIndexedScriptAction.INSTANCE, TransportDeleteIndexedScriptAction.class);

        registerAction(FieldStatsAction.INSTANCE, TransportFieldStatsTransportAction.class);
        */

        // register Name -> GenericAction Map that can be injected to instances.
        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);

        for (Map.Entry<String, ActionEntry> entry : actions.entrySet()) {
            actionsBinder.addBinding(entry.getKey()).toInstance(entry.getValue().action);
        }
    }
}

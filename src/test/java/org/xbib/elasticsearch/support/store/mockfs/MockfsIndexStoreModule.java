package org.xbib.elasticsearch.support.store.mockfs;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.store.IndexStore;

public class MockfsIndexStoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IndexStore.class).to(MockfsIndexStore.class).asEagerSingleton();
    }

}

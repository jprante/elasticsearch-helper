package org.xbib.elasticsearch.support.store.mockfs;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.store.IndicesStore;

public class MockfsIndexStore extends IndexStore {

    @Inject
    public MockfsIndexStore(Index index, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService,
                            IndicesStore indicesStore) {
        super(index, indexSettings, indexSettingsService, indicesStore);
    }

    @Override
    public Class<? extends DirectoryService> shardDirectory() {
        return MockfsDirectoryService.class;
    }

}

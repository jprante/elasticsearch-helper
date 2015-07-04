package org.xbib.elasticsearch.support.store.mockfs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreModule;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

public class MockfsDirectoryService extends FsDirectoryService {

    private final FsDirectoryService delegateService;
    private final Settings indexSettings;

    @Inject
    public MockfsDirectoryService(@IndexSettings Settings indexSettings, IndexStore indexStore, final ShardPath path) {
        super(indexSettings, indexStore, path);
        this.indexSettings = indexSettings;
        delegateService = directoryService(indexStore, path);
    }

    @Override
    public Directory newDirectory() throws IOException {
        return wrap(delegateService.newDirectory());
    }

    @Override
    protected synchronized Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onPause(long nanos) {
        delegateService.onPause(nanos);
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return delegateService.rateLimiting();
    }

    @Override
    public long throttleTimeInNanos() {
        return delegateService.throttleTimeInNanos();
    }

    private Directory wrap(Directory dir) {
        return new ElasticsearchMockDirectoryWrapper(dir);
    }

    private FsDirectoryService directoryService(IndexStore indexStore, ShardPath path) {
        Settings.Builder builder = Settings.settingsBuilder()
                .put(indexSettings)
                .put(IndexStoreModule.STORE_TYPE, IndexStoreModule.Type.DEFAULT); // TODO memory store, RAM codec?
        return new FsDirectoryService(builder.build(), indexStore, path);
    }

    class ElasticsearchMockDirectoryWrapper extends MockDirectoryWrapper {

        private final Set<String> superUnSyncedFiles;

        @SuppressWarnings("unchecked")
        public ElasticsearchMockDirectoryWrapper(Directory delegate) {
            super(delegate);
            try {
                Field field = MockDirectoryWrapper.class.getDeclaredField("unSyncedFiles");
                field.setAccessible(true);
                superUnSyncedFiles = (Set<String>) field.get(this);
            } catch (ReflectiveOperationException roe) {
                throw new RuntimeException(roe);
            }
        }

        private boolean mustSync() {
            Directory delegate = in;
            while (delegate instanceof FilterDirectory) {
                if (delegate instanceof NRTCachingDirectory) {
                    return true;
                }
                delegate = ((FilterDirectory) delegate).getDelegate();
            }
            return false;
        }

        @Override
        public synchronized void sync(Collection<String> names) throws IOException {
            if (mustSync()) {
                super.sync(names);
            } else {
                superUnSyncedFiles.removeAll(names);
            }
        }

    }
}

package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.Method;

/**
 * Performs a settings-based invocation.
 */
public interface RemoteInvoker extends AutoCloseable {

    /**
     * Performs a remote invocation
     *
     * @param settings the {@link Settings}
     * @param method the original {@link Method} that triggered the remote invocation
     * @param args the arguments of the remote invocation
     *
     * @return the {@link Future} that notifies the result of the remote invocation.
     */
    <T> Future<T> invoke(Settings settings, Method method, Object... args) throws Exception;

    /**
     * Closes the underlying socket connection and releases its associated resources.
     */
    @Override
    void close();
}

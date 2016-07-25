package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.common.settings.Settings;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public final class ClientInvocationHandler implements InvocationHandler {

    private static final Object[] NO_ARGS = new Object[0];

    private final Settings settings;
    private final RemoteInvoker remoteInvoker;
    private final Class<?> interfaceClass;

    public ClientInvocationHandler(RemoteInvoker remoteInvoker, Class<?> interfaceClass,
                                   Settings settings) {
        this.settings = settings;
        this.remoteInvoker = remoteInvoker;
        this.interfaceClass = interfaceClass;
    }

    public Settings getSettings() {
        return settings;
    }

    public Class<?> interfaceClass() {
        return interfaceClass;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Class<?> declaringClass = method.getDeclaringClass();
        if (declaringClass == Object.class) {
            return invokeObjectMethod(proxy, method, args);
        }
        assert declaringClass == interfaceClass;
        return invokeClientMethod(method, args);
    }

    private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
        final String methodName = method.getName();

        switch (methodName) {
            case "toString":
                return interfaceClass.getSimpleName() + '(' + settings.getAsMap() + ')';
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new Error("unknown method: " + methodName);
        }
    }

    private Object invokeClientMethod(Method method, Object[] args) throws Throwable {
        if (args == null) {
            args = NO_ARGS;
        }
        Future<Object> resultFuture = remoteInvoker.invoke(settings, method, args);
        return method.getReturnType().isInstance(resultFuture) ? resultFuture : null;
    }
}

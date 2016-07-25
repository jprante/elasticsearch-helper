package org.xbib.elasticsearch.helper.client;

import java.net.URL;

public final class Clients {

    public static <T> T newClient(URL url, Class<T> interfaceClass) {
        return new ClientBuilder().build(url, interfaceClass);
    }
}

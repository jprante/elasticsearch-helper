package org.xbib.elasticsearch.support.client;

import org.elasticsearch.common.settings.Settings;

import java.net.URI;

public interface ClientBuilder {

    ClientBuilder newClient();

    ClientBuilder newClient(URI uri);

    ClientBuilder newClient(URI uri, Settings settings);
}

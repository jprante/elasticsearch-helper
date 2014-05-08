package org.xbib.elasticsearch.plugin.feeder;

import java.io.Reader;

public interface Tool extends Runnable {

    Tool readFrom(Reader reader);

    void schedule(Thread thread);

    void shutdown();

    Thread shutdownHook();

}

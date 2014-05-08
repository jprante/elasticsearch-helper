
package org.xbib.pipeline;

import org.xbib.metrics.MeterMetric;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * A pipeline.
 *
 * @param <T> the pipeline result type
 * @param <R> the pipeline request type
 */
public interface Pipeline<T,R extends PipelineRequest>
        extends Callable<T>, Closeable, Iterator<R> {

    /**
     * Return the pipeline metric.
     *
     * @return the pipeline metric
     */
    MeterMetric getMetric();

}

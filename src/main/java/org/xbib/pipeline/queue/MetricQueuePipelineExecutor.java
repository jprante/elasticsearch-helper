
package org.xbib.pipeline.queue;

import org.xbib.metrics.MeterMetric;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.PipelineSink;
import org.xbib.pipeline.element.PipelineElement;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MetricQueuePipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T,R>, E extends PipelineElement>
        extends QueuePipelineExecutor<T,R,P,E> {

    protected MeterMetric metric;

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> setConcurrency(int concurrency) {
        super.setConcurrency(concurrency);
        return this;
    }

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> setPipelineProvider(PipelineProvider<P> provider) {
        super.setPipelineProvider(provider);
        return this;
    }

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> setSink(PipelineSink<T> sink) {
        super.setSink(sink);
        return this;
    }

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> prepare() {
        super.prepare();
        return this;
    }

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> execute() {
        metric = new MeterMetric(5L, TimeUnit.SECONDS);
        super.execute();
        return this;
    }

    @Override
    public MetricQueuePipelineExecutor<T,R,P,E> waitFor()
            throws InterruptedException, ExecutionException, IOException {
        super.waitFor();
        metric.stop();
        return this;
    }

    public MeterMetric getMetric() {
        return metric;
    }
}

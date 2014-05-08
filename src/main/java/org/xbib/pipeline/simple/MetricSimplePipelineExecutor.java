
package org.xbib.pipeline.simple;

import org.xbib.metrics.MeterMetric;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.PipelineSink;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MetricSimplePipelineExecutor<T, R extends PipelineRequest, P extends Pipeline<T,R>>
        extends SimplePipelineExecutor<T,R,P> {

    protected MeterMetric metric;

    @Override
    public MetricSimplePipelineExecutor<T,R,P> setConcurrency(int concurrency) {
        super.setConcurrency(concurrency);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T,R,P> setPipelineProvider(PipelineProvider<P> provider) {
        super.setPipelineProvider(provider);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T,R,P> setSink(PipelineSink<T> sink) {
        super.setSink(sink);
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T,R,P> prepare() {
        super.prepare();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T,R,P> execute() {
        metric = new MeterMetric(5L, TimeUnit.SECONDS);
        super.execute();
        return this;
    }

    @Override
    public MetricSimplePipelineExecutor<T,R,P> waitFor()
            throws InterruptedException, ExecutionException {
        super.waitFor();
        metric.stop();
        return this;
    }

    public MeterMetric metric() {
        return metric;
    }
}

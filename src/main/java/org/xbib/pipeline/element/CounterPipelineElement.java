package org.xbib.pipeline.element;

import org.xbib.metrics.CounterMetric;
import org.xbib.pipeline.PipelineRequest;

public class CounterPipelineElement implements PipelineElement<CounterMetric>, PipelineRequest {

    private CounterMetric metric;

    @Override
    public CounterMetric get() {
        return metric;
    }

    @Override
    public CounterPipelineElement set(CounterMetric metric) {
        this.metric = metric;
        return this;
    }

    @Override
    public String toString() {
        return metric.toString();
    }
}

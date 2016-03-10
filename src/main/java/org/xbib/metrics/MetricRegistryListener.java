package org.xbib.metrics;

import java.util.EventListener;

/**
 * Listeners for events from the registry.  Listeners must be thread-safe.
 */
public interface MetricRegistryListener extends EventListener {
    /**
     * A no-op implementation of {@link MetricRegistryListener}.
     */
    abstract class Base implements MetricRegistryListener {
        @Override
        public void onGaugeAdded(MetricName name, Gauge<?> gauge) {
        }

        @Override
        public void onGaugeRemoved(MetricName name) {
        }

        @Override
        public void onCounterAdded(MetricName name, CountMetric counter) {
        }

        @Override
        public void onCounterRemoved(MetricName name) {
        }

        @Override
        public void onHistogramAdded(MetricName name, Histogram histogram) {
        }

        @Override
        public void onHistogramRemoved(MetricName name) {
        }

        @Override
        public void onMeterAdded(MetricName name, Meter meter) {
        }

        @Override
        public void onMeterRemoved(MetricName name) {
        }

        @Override
        public void onTimerAdded(MetricName name, Sampler sampler) {
        }

        @Override
        public void onTimerRemoved(MetricName name) {
        }
    }

    /**
     * Called when a {@link Gauge} is added to the registry.
     *
     * @param name  the gauge's name
     * @param gauge the gauge
     */
    void onGaugeAdded(MetricName name, Gauge<?> gauge);

    /**
     * Called when a {@link Gauge} is removed from the registry.
     *
     * @param name the gauge's name
     */
    void onGaugeRemoved(MetricName name);

    /**
     * Called when a {@link CountMetric} is added to the registry.
     *
     * @param name    the counter's name
     * @param counter the counter
     */
    void onCounterAdded(MetricName name, CountMetric counter);

    /**
     * Called when a {@link CountMetric} is removed from the registry.
     *
     * @param name the counter's name
     */
    void onCounterRemoved(MetricName name);

    /**
     * Called when a {@link Histogram} is added to the registry.
     *
     * @param name      the histogram's name
     * @param histogram the histogram
     */
    void onHistogramAdded(MetricName name, Histogram histogram);

    /**
     * Called when a {@link Histogram} is removed from the registry.
     *
     * @param name the histogram's name
     */
    void onHistogramRemoved(MetricName name);

    /**
     * Called when a {@link Meter} is added to the registry.
     *
     * @param name  the meter's name
     * @param meter the meter
     */
    void onMeterAdded(MetricName name, Meter meter);

    /**
     * Called when a {@link Meter} is removed from the registry.
     *
     * @param name the meter's name
     */
    void onMeterRemoved(MetricName name);

    /**
     * Called when a {@link Sampler} is added to the registry.
     *
     * @param name  the sampler's name
     * @param sampler the sampler
     */
    void onTimerAdded(MetricName name, Sampler sampler);

    /**
     * Called when a {@link Sampler} is removed from the registry.
     *
     * @param name the timer's name
     */
    void onTimerRemoved(MetricName name);
}

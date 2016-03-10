package org.xbib.metrics;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A sampler metric which aggregates timing durations and provides duration statistics, plus
 * throughput statistics via {@link Meter}.
 */
public class Sampler implements Metered, Sampling {

    private final Meter meter;
    private final Histogram histogram;
    private final Clock clock;

    /**
     * Creates a new {@link Sampler} using an {@link ExponentiallyDecayingReservoir} and the default
     * {@link Clock}.
     */
    public Sampler() {
        this(new ExponentiallyDecayingReservoir());
    }

    /**
     * Creates a new {@link Sampler} that uses the given {@link Reservoir}.
     *
     * @param reservoir the {@link Reservoir} implementation the sampler should use
     */
    public Sampler(Reservoir reservoir) {
        this(reservoir, Clock.defaultClock());
    }

    /**
     * Creates a new {@link Sampler} that uses the given {@link Reservoir} and {@link Clock}.
     *
     * @param reservoir the {@link Reservoir} implementation the sampler should use
     * @param clock     the {@link Clock} implementation the sampler should use
     */
    public Sampler(Reservoir reservoir, Clock clock) {
        this.meter = new Meter(clock);
        this.clock = clock;
        this.histogram = new Histogram(reservoir);
    }

    /**
     * Adds a recorded duration.
     *
     * @param duration the length of the duration
     * @param unit     the scale unit of {@code duration}
     */
    public void update(long duration, TimeUnit unit) {
        update(unit.toNanos(duration));
    }

    /**
     * Records the duration of event.
     *
     * @param event a {@link Callable} whose {@link Callable#call()} method implements a process
     *              whose duration should be timed
     * @param <T>   the type of the value returned by {@code event}
     * @return the value returned by {@code event}
     * @throws Exception if {@code event} throws an {@link Exception}
     */
    public <T> T time(Callable<T> event) throws Exception {
        final long startTime = clock.getTick();
        try {
            return event.call();
        } finally {
            update(clock.getTick() - startTime);
        }
    }

    /**
     * Times and records the duration of an event.
     *
     * @param event a {@link Runnable} whose {@link Runnable#run()} method implements a process
     *              whose duration should be timed
     */
    public void time(Runnable event) {
        long startTime = this.clock.getTick();
        try {
            event.run();
        } finally {
            update(this.clock.getTick() - startTime);
        }
    }

    /**
     * Returns a new {@link Context}.
     *
     * @return a new {@link Context}
     * @see Context
     */
    public Context time() {
        return new Context(this, clock);
    }

    @Override
    public long getCount() {
        return histogram.getCount();
    }

    @Override
    public double getFifteenMinuteRate() {
        return meter.getFifteenMinuteRate();
    }

    @Override
    public double getFiveMinuteRate() {
        return meter.getFiveMinuteRate();
    }

    @Override
    public double getMeanRate() {
        return meter.getMeanRate();
    }

    @Override
    public double getOneMinuteRate() {
        return meter.getOneMinuteRate();
    }

    @Override
    public Snapshot getSnapshot() {
        return histogram.getSnapshot();
    }

    private void update(long duration) {
        if (duration >= 0) {
            histogram.inc(duration);
            meter.mark();
        }
    }

    /**
     * A timing context.
     *
     * @see Sampler#time()
     */
    public static class Context implements Closeable {
        private final Sampler sampler;
        private final Clock clock;
        private final long startTime;

        private Context(Sampler sampler, Clock clock) {
            this.sampler = sampler;
            this.clock = clock;
            this.startTime = clock.getTick();
        }

        /**
         * Updates the sampler with the difference between current and start time. Call to this method will
         * not reset the start time. Multiple calls result in multiple updates.
         *
         * @return the elapsed time in nanoseconds
         */
        public long stop() {
            final long elapsed = clock.getTick() - startTime;
            sampler.update(elapsed, TimeUnit.NANOSECONDS);
            return elapsed;
        }

        /**
         * Equivalent to calling {@link #stop()}.
         */
        @Override
        public void close() {
            stop();
        }
    }
}

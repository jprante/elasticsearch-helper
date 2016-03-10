package org.xbib.elasticsearch.common.metrics;

import com.twitter.jsr166e.LongAdder;
import org.xbib.metrics.ExpWeightedMovingAverage;
import org.xbib.metrics.Metered;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A meter metric which measures mean throughput and one-, five-, and
 * fifteen-minute exponentially-weighted moving average throughputs.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class ElasticsearchMeterMetric implements Metered {

    private final static ScheduledExecutorService service = Executors.newScheduledThreadPool(3);
    private final ExpWeightedMovingAverage m1Rate = ExpWeightedMovingAverage.oneMinuteEWMA();
    private final ExpWeightedMovingAverage m5Rate = ExpWeightedMovingAverage.fiveMinuteEWMA();
    private final ExpWeightedMovingAverage m15Rate = ExpWeightedMovingAverage.fifteenMinuteEWMA();
    private final LongAdder count;
    private final long startTime;
    private ScheduledFuture<?> future;

    public ElasticsearchMeterMetric() {
        this.count = new LongAdder();
        this.startTime = System.nanoTime();
    }

    public void spawn(long intervalSeconds) {
        this.future = service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                tick();
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Updates the moving averages.
     */
    public void tick() {
        m1Rate.tick();
        m5Rate.tick();
        m15Rate.tick();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        count.add(n);
        m1Rate.update(n);
        m5Rate.update(n);
        m15Rate.update(n);
    }

    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public double getFifteenMinuteRate() {
        return m15Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getFiveMinuteRate() {
        return m5Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getMeanRate() {
        long count = getCount();
        if (count == 0) {
            return 0.0;
        } else {
            final long elapsed = System.nanoTime() - startTime;
            return convertNsRate(count / (double) elapsed);
        }
    }

    @Override
    public double getOneMinuteRate() {
        return m1Rate.getRate(TimeUnit.SECONDS);
    }

    public void stop() {
        future.cancel(false);
    }

    private double convertNsRate(double ratePerNs) {
        return ratePerNs * (double) TimeUnit.SECONDS.toNanos(1);
    }

}

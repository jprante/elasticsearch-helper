package org.xbib.elasticsearch.common;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.jvm.GarbageCollector;
import org.xbib.elasticsearch.common.jvm.JvmInfo;
import org.xbib.elasticsearch.common.jvm.MemoryPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class GcMonitor {

    private final static ESLogger logger = ESLoggerFactory.getLogger(GcMonitor.class.getName());
    private final boolean enabled;
    private final Map<String, GcThreshold> gcThresholds;

    private volatile ScheduledFuture<?> scheduledFuture;

    public GcMonitor(Settings settings) {
        this.enabled = settings.getAsBoolean("monitor.gc.enabled", false);
        TimeValue interval = settings.getAsTime("monitor.gc.interval", timeValueSeconds(1));
        this.gcThresholds = new HashMap<>();
        Map<String, Settings> gcThresholdGroups = settings.getGroups("monitor.gc.level");
        for (Map.Entry<String, Settings> entry : gcThresholdGroups.entrySet()) {
            String name = entry.getKey();
            TimeValue warn = entry.getValue().getAsTime("warn", null);
            TimeValue info = entry.getValue().getAsTime("info", null);
            TimeValue debug = entry.getValue().getAsTime("debug", null);
            if (warn == null || info == null || debug == null) {
                logger.warn("ignoring gc_threshold for [{}], missing warn/info/debug values", name);
            } else {
                gcThresholds.put(name, new GcThreshold(name, warn.millis(), info.millis(), debug.millis()));
            }
        }
        if (!gcThresholds.containsKey(JvmInfo.YOUNG)) {
            gcThresholds.put(JvmInfo.YOUNG, new GcThreshold(JvmInfo.YOUNG, 1000, 700, 400));
        }
        if (!gcThresholds.containsKey(JvmInfo.OLD)) {
            gcThresholds.put(JvmInfo.OLD, new GcThreshold(JvmInfo.OLD, 10000, 5000, 2000));
        }
        if (!gcThresholds.containsKey("default")) {
            gcThresholds.put("default", new GcThreshold("default", 10000, 5000, 2000));
        }
        logger.debug("enabled [{}], interval [{}], gc_threshold [{}]", enabled, interval, this.gcThresholds);
        if (enabled) {
            scheduledFuture = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new GcMonitorThread(), 0L, interval.seconds(), TimeUnit.SECONDS);
        }
    }

    public void close() {
        if (!enabled) {
            return;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private class GcThreshold {
        public final String name;
        public final long warnThreshold;
        public final long infoThreshold;
        public final long debugThreshold;

        GcThreshold(String name, long warnThreshold, long infoThreshold, long debugThreshold) {
            this.name = name;
            this.warnThreshold = warnThreshold;
            this.infoThreshold = infoThreshold;
            this.debugThreshold = debugThreshold;
        }

        @Override
        public String toString() {
            return "GcThreshold{" +
                    "name='" + name + '\'' +
                    ", warnThreshold=" + warnThreshold +
                    ", infoThreshold=" + infoThreshold +
                    ", debugThreshold=" + debugThreshold +
                    '}';
        }
    }


    private class GcMonitorThread implements Runnable {

        private JvmInfo jvmInfo = JvmInfo.getInstance();
        private JvmInfo.Stats lastJvmStats = jvmInfo.stats;
        private long seq = 0;

        public GcMonitorThread() {
        }

        @Override
        public void run() {
            try {
                monitorGc();
            } catch (Throwable t) {
                logger.debug("failed to monitor", t);
            }
        }

        private synchronized void monitorGc() {
            seq++;
            JvmInfo.Stats currentJvmStats = JvmInfo.readStats();

            for (int i = 0; i < currentJvmStats.getGc().getCollectors().length; i++) {
                GarbageCollector gc = currentJvmStats.getGc().getCollectors()[i];
                GarbageCollector prevGc = lastJvmStats.getGc().getCollectors()[i];

                long collections = gc.getCollectionCount() - prevGc.getCollectionCount();
                if (collections == 0) {
                    continue;
                }
                long collectionTime = gc.getCollectionTime().getMillis() - prevGc.getCollectionTime().getMillis();
                if (collectionTime == 0) {
                    continue;
                }

                GcThreshold gcThreshold = gcThresholds.get(gc.getName());
                if (gcThreshold == null) {
                    gcThreshold = gcThresholds.get("default");
                }

                long avgCollectionTime = collectionTime / collections;

                if (avgCollectionTime > gcThreshold.warnThreshold) {
                    logger.warn("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(),
                            seq,
                            gc.getCollectionCount(),
                            TimeValue.timeValueMillis(collectionTime),
                            collections,
                            TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()),
                            TimeValue.timeValueMillis(collectionTime),
                            gc.getCollectionTime(),
                            lastJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapMax(),
                            buildPools(lastJvmStats, currentJvmStats));
                } else if (avgCollectionTime > gcThreshold.infoThreshold) {
                    logger.info("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(),
                            seq,
                            gc.getCollectionCount(),
                            TimeValue.timeValueMillis(collectionTime),
                            collections,
                            TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()),
                            TimeValue.timeValueMillis(collectionTime),
                            gc.getCollectionTime(),
                            lastJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapMax(),
                            buildPools(lastJvmStats, currentJvmStats));
                } else if (avgCollectionTime > gcThreshold.debugThreshold && logger.isDebugEnabled()) {
                    logger.debug("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(),
                            seq,
                            gc.getCollectionCount(),
                            TimeValue.timeValueMillis(collectionTime),
                            collections,
                            TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()),
                            TimeValue.timeValueMillis(collectionTime),
                            gc.getCollectionTime(),
                            lastJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapUsed(),
                            currentJvmStats.getMem().getHeapMax(),
                            buildPools(lastJvmStats, currentJvmStats));
                }
            }
            lastJvmStats = currentJvmStats;
        }

        private String buildPools(JvmInfo.Stats prev, JvmInfo.Stats current) {
            StringBuilder sb = new StringBuilder();
            for (MemoryPool currentPool : current.getMem()) {
                MemoryPool prevPool = null;
                for (MemoryPool pool : prev.getMem()) {
                    if (pool.getName().equals(currentPool.getName())) {
                        prevPool = pool;
                        break;
                    }
                }
                sb.append("{[").append(currentPool.getName())
                        .append("] [").append(prevPool == null ? "?" : prevPool.getUsed()).append("]->[").append(currentPool.getUsed()).append("]/[").append(currentPool.getMax()).append("]}");
            }
            return sb.toString();
        }
    }
}

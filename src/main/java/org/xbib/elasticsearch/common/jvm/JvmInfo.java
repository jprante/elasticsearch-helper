package org.xbib.elasticsearch.common.jvm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JvmInfo implements Streamable, ToXContent {

    public static final String YOUNG = "young";
    public static final String OLD = "old";
    public static final String SURVIVOR = "survivor";
    private final static RuntimeMXBean runtimeMXBean;
    private final static MemoryMXBean memoryMXBean;
    private final static ThreadMXBean threadMXBean;
    private final static ClassLoadingMXBean classLoadingMXBean;
    private static JvmInfo INSTANCE;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        long pid;
        String xPid = runtimeMXBean.getName();
        try {
            xPid = xPid.split("@")[0];
            pid = Long.parseLong(xPid);
        } catch (Exception e) {
            pid = -1;
        }
        JvmInfo info = new JvmInfo();
        info.pid = pid;
        info.startTime = runtimeMXBean.getStartTime();
        info.version = runtimeMXBean.getSystemProperties().get("java.version");
        info.vmName = runtimeMXBean.getVmName();
        info.vmVendor = runtimeMXBean.getVmVendor();
        info.vmVersion = runtimeMXBean.getVmVersion();
        /* TODO without sun.misc.VM, read from argument line somehow?
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            info.mem.directMemoryMax = (Long) vmClass.getMethod("maxDirectMemory").invoke(null);
        } catch (Throwable t) {
            // ignore
        }*/
        info.inputArguments = runtimeMXBean.getInputArguments().toArray(new String[runtimeMXBean.getInputArguments().size()]);
        info.bootClassPath = runtimeMXBean.getBootClassPath();
        info.classPath = runtimeMXBean.getClassPath();
        info.systemProperties = runtimeMXBean.getSystemProperties();

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        info.gcCollectors = new String[gcMxBeans.size()];
        for (int i = 0; i < gcMxBeans.size(); i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            info.gcCollectors[i] = gcMxBean.getName();
        }

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        info.memoryPools = new String[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            info.memoryPools[i] = memoryPoolMXBean.getName();
        }
        info.stats = readStats();

        INSTANCE = info;
    }

    public Stats stats;
    long pid = -1;
    String version = "";
    String vmName = "";
    String vmVersion = "";
    String vmVendor = "";
    long startTime = -1;
    String[] inputArguments;
    String bootClassPath;
    String classPath;
    Map<String, String> systemProperties;
    String[] gcCollectors = Strings.EMPTY_ARRAY;
    String[] memoryPools = Strings.EMPTY_ARRAY;

    private JvmInfo() {
    }

    public static JvmInfo getInstance() {
        return INSTANCE;
    }

    public static Stats readStats() {
        Stats stats = new Stats();
        stats.timestamp = System.currentTimeMillis();
        stats.uptime = runtimeMXBean.getUptime();
        stats.mem = new Mem();

        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        stats.mem.heapInit = memUsage.getInit() < 0 ? 0 : memUsage.getInit();
        stats.mem.heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        stats.mem.heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();

        memUsage = memoryMXBean.getNonHeapMemoryUsage();
        stats.mem.nonHeapInit = memUsage.getInit() < 0 ? 0 : memUsage.getInit();
        stats.mem.nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        stats.mem.nonHeapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        List<MemoryPool> pools = new ArrayList<>();
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            try {
                MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
                MemoryUsage usage = memoryPoolMXBean.getUsage();
                MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
                String name = getByMemoryPoolName(memoryPoolMXBean.getName(), null);
                if (name == null) { // if we can't resolve it, its not interesting.... (Per Gen, Code Cache)
                    continue;
                }
                pools.add(new MemoryPool(name,
                        usage.getUsed() < 0 ? 0 : usage.getUsed(),
                        usage.getMax() < 0 ? 0 : usage.getMax(),
                        peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                        peakUsage.getMax() < 0 ? 0 : peakUsage.getMax()
                ));
            } catch (OutOfMemoryError err) {
                throw err; // rethrow
            } catch (Throwable ex) {
                /* ignore some JVMs might barf here with:
                 * java.lang.InternalError: Memory Pool not found
                 * we just omit the pool in that case!*/
            }
        }
        stats.mem.pools = pools.toArray(new MemoryPool[pools.size()]);

        stats.threads = new Threads();
        stats.threads.count = threadMXBean.getThreadCount();
        stats.threads.peakCount = threadMXBean.getPeakThreadCount();



        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        stats.gc = new GarbageCollectors();
        stats.gc.collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < stats.gc.collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            stats.gc.collectors[i] = new GarbageCollector();
            stats.gc.collectors[i].name = getByGcName(gcMxBean.getName(), gcMxBean.getName());
            stats.gc.collectors[i].collectionCount = gcMxBean.getCollectionCount();
            stats.gc.collectors[i].collectionTime = gcMxBean.getCollectionTime();
        }

        try {
            List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            stats.bufferPools = new ArrayList<>(bufferPools.size());
            for (BufferPoolMXBean bufferPool : bufferPools) {
                stats.bufferPools.add(new BufferPool(bufferPool.getName(), bufferPool.getCount(), bufferPool.getTotalCapacity(), bufferPool.getMemoryUsed()));
            }
        } catch (Throwable t) {
            // buffer pools are not available
        }

        stats.classes = new Classes();
        stats.classes.loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        stats.classes.totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        stats.classes.unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

        return stats;
    }

    public static String getByMemoryPoolName(String poolName, String defaultName) {
        if ("Eden Space".equals(poolName) || "PS Eden Space".equals(poolName) || "Par Eden Space".equals(poolName) || "G1 Eden Space".equals(poolName)) {
            return YOUNG;
        }
        if ("Survivor Space".equals(poolName) || "PS Survivor Space".equals(poolName) || "Par Survivor Space".equals(poolName) || "G1 Survivor Space".equals(poolName)) {
            return SURVIVOR;
        }
        if ("Tenured Gen".equals(poolName) || "PS Old Gen".equals(poolName) || "CMS Old Gen".equals(poolName) || "G1 Old Gen".equals(poolName)) {
            return OLD;
        }
        return defaultName;
    }

    public static String getByGcName(String gcName, String defaultName) {
        if ("Copy".equals(gcName) || "PS Scavenge".equals(gcName) || "ParNew".equals(gcName) || "G1 Young Generation".equals(gcName)) {
            return YOUNG;
        }
        if ("MarkSweepCompact".equals(gcName) || "PS MarkSweep".equals(gcName) || "ConcurrentMarkSweep".equals(gcName) || "G1 Old Generation".equals(gcName)) {
            return OLD;
        }
        return defaultName;
    }

    public static JvmInfo readJvmInfo(StreamInput in) throws IOException {
        JvmInfo jvmInfo = new JvmInfo();
        jvmInfo.readFrom(in);
        return jvmInfo;
    }

    public long pid() {
        return this.pid;
    }

    public long getPid() {
        return pid;
    }

    public String version() {
        return this.version;
    }

    public String getVersion() {
        return this.version;
    }

    public int versionAsInteger() {
        try {
            int i = 0;
            String sVersion = "";
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion += version.charAt(i);
                }
            }
            if (i == 0) {
                return -1;
            }
            return Integer.parseInt(sVersion);
        } catch (Exception e) {
            return -1;
        }
    }

    public int versionUpdatePack() {
        try {
            int i = 0;
            String sVersion = "";
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion += version.charAt(i);
                }
            }
            if (i == 0) {
                return -1;
            }
            Integer.parseInt(sVersion);
            int from;
            if (version.charAt(i) == '_') {
                // 1.7.0_4
                from = ++i;
            } else if (version.charAt(i) == '-' && version.charAt(i + 1) == 'u') {
                // 1.7.0-u2-b21
                i = i + 2;
                from = i;
            } else {
                return -1;
            }
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
            }
            if (from == i) {
                return -1;
            }
            return Integer.parseInt(version.substring(from, i));
        } catch (Exception e) {
            return -1;
        }
    }

    public String getVmName() {
        return this.vmName;
    }

    public String getVmVersion() {
        return this.vmVersion;
    }

    public String getVmVendor() {
        return this.vmVendor;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public String[] getInputArguments() {
        return this.inputArguments;
    }

    public String getBootClassPath() {
        return this.bootClassPath;
    }

    public String getClassPath() {
        return this.classPath;
    }

    public Map<String, String> getSystemProperties() {
        return this.systemProperties;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.TIMESTAMP, stats.timestamp);
        builder.dateValueField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, startTime);
        builder.timeValueField(Fields.UPTIME_IN_MILLIS, Fields.UPTIME, stats.uptime);
        builder.field(Fields.PID, pid);
        builder.field(Fields.VERSION, version);
        builder.field(Fields.VM_NAME, vmName);
        builder.field(Fields.VM_VERSION, vmVersion);
        builder.field(Fields.VM_VENDOR, vmVendor);

        if (stats.threads != null) {
            builder.startObject(Fields.THREADS);
            builder.field(Fields.COUNT, stats.threads.getCount());
            builder.field(Fields.PEAK_COUNT, stats.threads.getPeakCount());
            builder.endObject();
        }

        builder.field(Fields.MEMORY_POOLS, memoryPools);

        if (stats.mem != null) {
            builder.startObject(Fields.MEM);
            builder.startObject(Fields.POOLS);
            for (MemoryPool pool : stats.mem) {
                builder.startObject(pool.getName(), XContentBuilder.FieldCaseConversion.NONE);
                builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, pool.used);
                builder.byteSizeField(Fields.MAX_IN_BYTES, Fields.MAX, pool.max);
                builder.byteSizeField(Fields.PEAK_USED_IN_BYTES, Fields.PEAK_USED, pool.peakUsed);
                builder.byteSizeField(Fields.PEAK_MAX_IN_BYTES, Fields.PEAK_MAX, pool.peakMax);
                builder.endObject();
            }
            builder.endObject();
            if (stats.mem.getHeapUsedPercent() >= 0) {
                builder.field(Fields.HEAP_USED_PERCENT, stats.mem.getHeapUsedPercent());
            }
            builder.byteSizeField(Fields.HEAP_INIT_IN_BYTES, Fields.HEAP_INIT, stats.mem.heapInit);
            builder.byteSizeField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, stats.mem.heapUsed);
            builder.byteSizeField(Fields.HEAP_COMMITTED_IN_BYTES, Fields.HEAP_COMMITTED, stats.mem.heapCommitted);
            builder.byteSizeField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, stats.mem.heapMax);
            builder.byteSizeField(Fields.NON_HEAP_INIT_IN_BYTES, Fields.NON_HEAP_INIT, stats.mem.nonHeapInit);
            builder.byteSizeField(Fields.NON_HEAP_USED_IN_BYTES, Fields.NON_HEAP_USED, stats.mem.nonHeapUsed);
            builder.byteSizeField(Fields.NON_HEAP_COMMITTED_IN_BYTES, Fields.NON_HEAP_COMMITTED, stats.mem.nonHeapCommitted);
            builder.byteSizeField(Fields.NON_HEAP_MAX_IN_BYTES, Fields.NON_HEAP_MAX, stats.mem.nonHeapMax);
            builder.byteSizeField(Fields.DIRECT_MAX_IN_BYTES, Fields.DIRECT_MAX, stats.mem.directMemoryMax);
            builder.endObject();
        }

        builder.field(Fields.GC_COLLECTORS, gcCollectors);

        if (stats.gc != null) {
            builder.startObject(Fields.GC);

            builder.startObject(Fields.COLLECTORS);
            for (GarbageCollector collector : stats.gc) {
                builder.startObject(collector.getName(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.COLLECTION_COUNT, collector.getCollectionCount());
                builder.timeValueField(Fields.COLLECTION_TIME_IN_MILLIS, Fields.COLLECTION_TIME, collector.collectionTime);
                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }

        if (stats.bufferPools != null) {
            builder.startObject(Fields.BUFFER_POOLS);
            for (BufferPool bufferPool : stats.bufferPools) {
                builder.startObject(bufferPool.getName(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.COUNT, bufferPool.getCount());
                builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, bufferPool.used);
                builder.byteSizeField(Fields.TOTAL_CAPACITY_IN_BYTES, Fields.TOTAL_CAPACITY, bufferPool.totalCapacity);
                builder.endObject();
            }
            builder.endObject();
        }

        if (stats.classes != null) {
            builder.startObject(Fields.CLASSES);
            builder.field(Fields.CURRENT_LOADED_COUNT, stats.classes.getLoadedClassCount());
            builder.field(Fields.TOTAL_LOADED_COUNT, stats.classes.getTotalLoadedClassCount());
            builder.field(Fields.TOTAL_UNLOADED_COUNT, stats.classes.getUnloadedClassCount());
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        pid = in.readLong();
        version = in.readString();
        vmName = in.readString();
        vmVersion = in.readString();
        vmVendor = in.readString();
        startTime = in.readLong();
        inputArguments = new String[in.readInt()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readString();
        }
        bootClassPath = in.readString();
        classPath = in.readString();
        systemProperties = new HashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            systemProperties.put(in.readString(), in.readString());
        }
        stats.timestamp = in.readVLong();
        stats.uptime = in.readVLong();

        stats.threads = Threads.readThreads(in);
        memoryPools = in.readStringArray();
        stats.mem = Mem.readMem(in);
        gcCollectors = in.readStringArray();
        stats.gc = GarbageCollectors.readGarbageCollectors(in);

        if (in.readBoolean()) {
            size = in.readVInt();
            stats.bufferPools = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BufferPool bufferPool = new BufferPool();
                bufferPool.readFrom(in);
                stats.bufferPools.add(bufferPool);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(pid);
        out.writeString(version);
        out.writeString(vmName);
        out.writeString(vmVersion);
        out.writeString(vmVendor);
        out.writeLong(startTime);
        out.writeInt(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeString(inputArgument);
        }
        out.writeString(bootClassPath);
        out.writeString(classPath);
        out.writeInt(systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVLong(stats.timestamp);
        out.writeVLong(stats.uptime);

        stats.threads.writeTo(out);
        out.writeStringArray(memoryPools);
        stats.mem.writeTo(out);
        out.writeStringArray(gcCollectors);
        stats.gc.writeTo(out);

        if (stats.bufferPools == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(stats.bufferPools.size());
            for (BufferPool bufferPool : stats.bufferPools) {
                bufferPool.writeTo(out);
            }
        }
    }

    public static class Stats {

        long timestamp = -1;
        long uptime;
        Threads threads;
        Mem mem;
        GarbageCollectors gc;
        List<BufferPool> bufferPools;
        Classes classes;


        public long getTimestamp() {
            return timestamp;
        }

        public Mem getMem() {
            return mem;
        }

        public GarbageCollectors getGc() {
            return gc;
        }

    }

    static final class Fields {
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString PID = new XContentBuilderString("pid");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString VM_NAME = new XContentBuilderString("vm_name");
        static final XContentBuilderString VM_VERSION = new XContentBuilderString("vm_version");
        static final XContentBuilderString VM_VENDOR = new XContentBuilderString("vm_vendor");

        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString START_TIME_IN_MILLIS = new XContentBuilderString("start_time_in_millis");
        static final XContentBuilderString UPTIME = new XContentBuilderString("uptime");
        static final XContentBuilderString UPTIME_IN_MILLIS = new XContentBuilderString("uptime_in_millis");

        static final XContentBuilderString THREADS = new XContentBuilderString("threads");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString PEAK_COUNT = new XContentBuilderString("peak_count");

        static final XContentBuilderString DIRECT_MAX = new XContentBuilderString("direct_max");
        static final XContentBuilderString DIRECT_MAX_IN_BYTES = new XContentBuilderString("direct_max_in_bytes");

        static final XContentBuilderString MEMORY_POOLS = new XContentBuilderString("memory_pools");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");

        static final XContentBuilderString HEAP_INIT = new XContentBuilderString("heap_init");
        static final XContentBuilderString HEAP_INIT_IN_BYTES = new XContentBuilderString("heap_init_in_bytes");
        static final XContentBuilderString HEAP_USED = new XContentBuilderString("heap_used");
        static final XContentBuilderString HEAP_USED_IN_BYTES = new XContentBuilderString("heap_used_in_bytes");
        static final XContentBuilderString HEAP_COMMITTED = new XContentBuilderString("heap_committed");
        static final XContentBuilderString HEAP_COMMITTED_IN_BYTES = new XContentBuilderString("heap_committed_in_bytes");
        static final XContentBuilderString HEAP_MAX = new XContentBuilderString("heap_max");
        static final XContentBuilderString HEAP_MAX_IN_BYTES = new XContentBuilderString("heap_max_in_bytes");

        static final XContentBuilderString HEAP_USED_PERCENT = new XContentBuilderString("heap_used_percent");

        static final XContentBuilderString NON_HEAP_INIT = new XContentBuilderString("non_heap_init");
        static final XContentBuilderString NON_HEAP_INIT_IN_BYTES = new XContentBuilderString("non_heap_init_in_bytes");
        static final XContentBuilderString NON_HEAP_USED = new XContentBuilderString("non_heap_used");
        static final XContentBuilderString NON_HEAP_USED_IN_BYTES = new XContentBuilderString("non_heap_used_in_bytes");
        static final XContentBuilderString NON_HEAP_COMMITTED = new XContentBuilderString("non_heap_committed");
        static final XContentBuilderString NON_HEAP_COMMITTED_IN_BYTES = new XContentBuilderString("non_heap_committed_in_bytes");
        static final XContentBuilderString NON_HEAP_MAX = new XContentBuilderString("non_heap_max");
        static final XContentBuilderString NON_HEAP_MAX_IN_BYTES = new XContentBuilderString("non_heap_max_in_bytes");

        static final XContentBuilderString POOLS = new XContentBuilderString("pools");
        static final XContentBuilderString USED = new XContentBuilderString("used");
        static final XContentBuilderString USED_IN_BYTES = new XContentBuilderString("used_in_bytes");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString MAX_IN_BYTES = new XContentBuilderString("max_in_bytes");
        static final XContentBuilderString PEAK_USED = new XContentBuilderString("peak_used");
        static final XContentBuilderString PEAK_USED_IN_BYTES = new XContentBuilderString("peak_used_in_bytes");
        static final XContentBuilderString PEAK_MAX = new XContentBuilderString("peak_max");
        static final XContentBuilderString PEAK_MAX_IN_BYTES = new XContentBuilderString("peak_max_in_bytes");

        static final XContentBuilderString GC_COLLECTORS = new XContentBuilderString("gc_collectors");

        static final XContentBuilderString GC = new XContentBuilderString("gc");
        static final XContentBuilderString COLLECTORS = new XContentBuilderString("collectors");
        static final XContentBuilderString COLLECTION_COUNT = new XContentBuilderString("collection_count");
        static final XContentBuilderString COLLECTION_TIME = new XContentBuilderString("collection_time");
        static final XContentBuilderString COLLECTION_TIME_IN_MILLIS = new XContentBuilderString("collection_time_in_millis");

        static final XContentBuilderString BUFFER_POOLS = new XContentBuilderString("buffer_pools");
        static final XContentBuilderString TOTAL_CAPACITY = new XContentBuilderString("total_capacity");
        static final XContentBuilderString TOTAL_CAPACITY_IN_BYTES = new XContentBuilderString("total_capacity_in_bytes");

        static final XContentBuilderString CLASSES = new XContentBuilderString("classes");
        static final XContentBuilderString CURRENT_LOADED_COUNT = new XContentBuilderString("current_loaded_count");
        static final XContentBuilderString TOTAL_LOADED_COUNT = new XContentBuilderString("total_loaded_count");
        static final XContentBuilderString TOTAL_UNLOADED_COUNT = new XContentBuilderString("total_unloaded_count");

    }

}

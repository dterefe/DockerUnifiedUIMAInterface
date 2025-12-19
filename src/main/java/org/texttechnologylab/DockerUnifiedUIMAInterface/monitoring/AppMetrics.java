package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Enumeration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.hotspot.DefaultExports;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import one.profiler.AsyncProfiler;

public final class AppMetrics implements AutoCloseable {
    private static final String NONE = "(none)";

    private enum ProfileMode {
        CPU,
        NO_CPU
    }

    private static final AsyncProfiler PROFILER = AsyncProfiler.getInstance();

    // --- Step/Substep ---
    private static final Histogram STEP_LATENCY = Histogram.build()
            .name("app_step_latency")                       // Basisname
            .help("Latency per step/component in ms")
            .labelNames("step", "component")
            .unit("milliseconds")                           // hängt _milliseconds an den Namen an
            .buckets(1, 2, 5, 10, 50, 100, 200, 300, 400, 500,
                    600, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400, 2600, 2800, 3000, 3200, 3400, 3600, 3800, 4000,
                    4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
                    6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000,
                    8200, 8400, 8600, 8800, 9000, 9200, 9400, 9600, 9800, 10000,
                    10200, 10400, 10600, 10800, 11000, 11200, 11400, 11600, 11800, 12000,
                    12200, 12400, 12600, 12800, 13000, 13200, 13400, 13600, 13800, 14000,
                    14200, 14400, 14600, 14800, 15000, 15200, 15400, 15600, 15800, 16000,
                    16200, 16400, 16600, 16800, 17000, 17200, 17400, 17600, 17800, 18000,
                    18200, 18400, 18600, 18800, 19000, 19200, 19400, 19600, 19800, 20000)
            .register();

    private static final Counter STEP_ERRORS = Counter.build()
            .name("app_step_errors_total").help("Errors per step/component")
            .labelNames("step", "component").register();

    // --- Step heap usage ---
    private static final Gauge STEP_HEAP_USED_BYTES = Gauge.build()
            .name("app_step_heap_used_bytes")
            .help("Heap used at end of step/component")
            .labelNames("step", "component")
            .register();

    // --- Step CPU per component ---
    private static final Histogram STEP_CPU = Histogram.build()
            .name("app_step_cpu")
            .help("CPU time per step/component in ms")
            .labelNames("step", "component")
            .unit("milliseconds")
            .buckets(1, 5, 10, 50, 100, 200, 300, 400, 500,
                    600, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400, 2600, 2800, 3000, 3200, 3400, 3600, 3800, 4000,
                    4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
                    6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000,
                    8200, 8400, 8600, 8800, 9000, 9200, 9400, 9600, 9800, 10000)
            .register();

    // --- Document total ---
    private static final Histogram DOC_TOTAL = Histogram.build()
            .name("app_document_total")
            .help("Total document processing time in ms")
            .labelNames("result") // ok|error
            .unit("milliseconds")
            .buckets(1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400, 2600, 2800, 3000, 3200, 3400, 3600, 3800, 4000,
                    4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
                    6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000,
                    8200, 8400, 8600, 8800, 9000, 9200, 9400, 9600, 9800, 10000,
                    10200, 10400, 10600, 10800, 11000, 11200, 11400, 11600, 11800, 12000,
                    12200, 12400, 12600, 12800, 13000, 13200, 13400, 13600, 13800, 14000,
                    14200, 14400, 14600, 14800, 15000, 15200, 15400, 15600, 15800, 16000,
                    16200, 16400, 16600, 16800, 17000, 17200, 17400, 17600, 17800, 18000,
                    18200, 18400, 18600, 18800, 19000, 19200, 19400, 19600, 19800, 20000)
            .register();

    private static final Counter DOC_ERRORS = Counter.build()
            .name("app_document_errors_total").help("Errors per document").register();

    // --- Document heap usage ---
    private static final Gauge DOC_HEAP_USED_BYTES = Gauge.build()
            .name("app_document_heap_used_bytes")
            .help("Heap used at end of document")
            .labelNames("result", "size_bucket")
            .register();

    // --- Per-worker document metrics ---
    private static final Counter WORKER_DOCS_PROCESSED = Counter.build()
            .name("duui_worker_docs_processed_total")
            .help("Documents processed per DUUI worker thread")
            .labelNames("thread")
            .register();

    private static final Histogram WORKER_DOC_LATENCY = Histogram.build()
            .name("duui_worker_document_latency")
            .help("Document processing time per DUUI worker thread in ms")
            .labelNames("thread")
            .unit("milliseconds")
            .buckets(10, 50, 100, 200, 500, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400, 2600, 2800, 3000, 3200, 3400, 3600, 3800, 4000,
                    4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
                    6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000,
                    8200, 8400, 8600, 8800, 9000, 9200, 9400, 9600, 9800, 10000,
                    10200, 10400, 10600, 10800, 11000, 11200, 11400, 11600, 11800, 12000,
                    12200, 12400, 12600, 12800, 13000, 13200, 13400, 13600, 13800, 14000,
                    14200, 14400, 14600, 14800, 15000)
            .register();

    // --- Document CPU per size bucket ---
    private static final Histogram DOC_CPU_BY_SIZE = Histogram.build()
            .name("duui_document_cpu_by_size")
            .help("Document CPU time per size bucket in ms")
            .labelNames("size_bucket")
            .unit("milliseconds")
            .buckets(10, 50, 100, 200, 500, 1000, 1200, 1400, 1600, 1800, 2000,
                    2200, 2400, 2600, 2800, 3000, 3200, 3400, 3600, 3800, 4000,
                    4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
                    6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000,
                    8200, 8400, 8600, 8800, 9000, 9200, 9400, 9600, 9800, 10000,
                    10200, 10400, 10600, 10800, 11000, 11200, 11400, 11600, 11800, 12000,
                    12200, 12400, 12600, 12800, 13000, 13200, 13400, 13600, 13800, 14000,
                    14200, 14400, 14600, 14800, 15000)
            .register();
    // --- System metrics ---

    private static final Gauge MEM_SYSTEM_TOTAL = Gauge.build()
            .name("host_memory_total_bytes").help("Total physical memory").register();

    // --- state ---
    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-sampler"); t.setDaemon(true); return t;
    });
    private final long periodMillis;
    private final String runName;
    private final Path outputDirectory;
    private final ProfileMode profileMode;
    private ScheduledFuture<?> sysTask;

    // OSHI
    private final SystemInfo si = new SystemInfo();
    private final CentralProcessor cpu = si.getHardware().getProcessor();
    private final GlobalMemory mem = si.getHardware().getMemory();
    private final OperatingSystem os = si.getOperatingSystem();
    private long[] prevTicks = cpu.getSystemCpuLoadTicks();
    private OSProcess prevProc = os.getProcess(os.getProcessId());

    private static final ThreadMXBean THREAD_CPU_BEAN = ManagementFactory.getThreadMXBean();
    private static final java.lang.management.MemoryMXBean MEM_BEAN = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean tBean = ManagementFactory.getThreadMXBean();
    private final java.util.Set<Long> trackedThreadIds =
        java.util.Collections.synchronizedSet(new java.util.HashSet<>());
    private volatile long[] trackedThreadIdArray;
    private final java.util.Map<Long, Long> prevThreadCpuNanos = new java.util.HashMap<>();
    private final java.util.Map<Long, Long> prevBlockedTime = new java.util.HashMap<>();
    private final java.util.Map<Long, Long> prevWaitedTime = new java.util.HashMap<>();
    private final java.util.Map<String, Long> prevGcCount = new java.util.HashMap<>();
    private final java.util.Map<String, Long> prevGcTimeMs = new java.util.HashMap<>();
    private final java.util.Map<String, Long> prevNetBytesRecv = new java.util.HashMap<>();
    private final java.util.Map<String, Long> prevNetBytesSent = new java.util.HashMap<>();
    private long prevCpuInterrupts = -1L;
    private long prevCpuCtxSwitches = -1L;
    private final java.util.List<Long> ts = new java.util.ArrayList<>();
    private final java.util.Map<String, java.util.List<Double>> series = new java.util.LinkedHashMap<>();

    private static String keyOf(String metric, java.util.Map<String,String> labels) {
        if (labels==null || labels.isEmpty()) return metric;
        var keys = new java.util.ArrayList<>(labels.keySet()); java.util.Collections.sort(keys);
        var sb = new StringBuilder(metric);
        for (String k: keys) sb.append('|').append(k).append('=').append(labels.get(k));
        return sb.toString();
    }
    private void measure(String metric, java.util.Map<String,String> labels, double value) {
        String key = keyOf(metric, labels);
        series.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(value);
    }


    public AppMetrics(String runName, Path outputDirectory) {
        this(runName, outputDirectory, ProfileMode.CPU);
    }

    public AppMetrics(String runName, String outputDirectory) {
        this(runName, Path.of(outputDirectory));
    }

    public AppMetrics(String runName, Path outputDirectory, ProfileMode profileMode) {
        this.periodMillis = Duration.ofSeconds(2).toMillis();
        this.runName = runName;
        this.outputDirectory = outputDirectory;
        this.profileMode = profileMode;
        DefaultExports.initialize();                 // JVM-Metriken
        MEM_SYSTEM_TOTAL.set(mem.getTotal());       // einmalig
    }

    /**
     * Register a thread whose state should be tracked by the sampler.
     * <p>
     * If no threads are registered, the sampler falls back to tracking all JVM
     * threads (legacy behaviour).
     *
     * @param thread Thread to track
     */
    public void addThread(Thread thread) {
        if (thread == null) {
            return;
        }

        long id = thread.getId();
        synchronized (trackedThreadIds) {
            // Only rebuild the cached array when a new ID is actually added
            if (trackedThreadIds.add(id)) {
                long[] arr = new long[trackedThreadIds.size()];
                int i = 0;
                for (Long tid : trackedThreadIds) {
                    arr[i++] = tid;
                }
                trackedThreadIdArray = arr;
            }
        }
    }

    // lifecycle
    public synchronized void start() {
        if (tBean.isThreadContentionMonitoringSupported()) {
            tBean.setThreadContentionMonitoringEnabled(true);
        }
        if (sysTask == null || sysTask.isCancelled()) {
            startProfiler();
            sysTask = ses.scheduleAtFixedRate(this::sampleSafely, 0, periodMillis, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void stop() {
        if (sysTask != null) { sysTask.cancel(false); sysTask = null; }
        stopProfiler();
        try { writeSnapshotJson(); } catch (Exception ignore) {}
        ses.shutdown();
        ts.clear();
        series.clear();
        trackedThreadIds.clear();
        trackedThreadIdArray = null;
        prevThreadCpuNanos.clear();
        prevBlockedTime.clear();
        prevWaitedTime.clear();
        prevGcCount.clear();
        prevGcTimeMs.clear();
        prevNetBytesRecv.clear();
        prevNetBytesSent.clear();
        prevCpuInterrupts = -1L;
        prevCpuCtxSwitches = -1L;
        STEP_LATENCY.clear();
        STEP_CPU.clear();
        DOC_TOTAL.clear();
        STEP_ERRORS.clear();
        DOC_ERRORS.clear();
        STEP_HEAP_USED_BYTES.clear();
        DOC_HEAP_USED_BYTES.clear();
        WORKER_DOCS_PROCESSED.clear();
        WORKER_DOC_LATENCY.clear();
        DOC_CPU_BY_SIZE.clear();
    }

    @Override public void close() { stop(); }

    // step timers
    public static Timer timeStep(String step) { return new Timer(step, NONE); }
    public static Timer timeStep(String step, String component) { return new Timer(step, component); }

    // document total
    public static DocRun docRun() { return new DocRun(null); }
    public static DocRun docRun(String sizeBucket) { return new DocRun(sizeBucket); }

    public static final class Timer implements AutoCloseable {
        private final String step, comp;
        private final long t0 = System.nanoTime();
        private final long cpu0 = THREAD_CPU_BEAN.getCurrentThreadCpuTime();

        Timer(String step, String comp) {
            this.step = step; this.comp = comp;
        }

        @Override public void close() {
            double ms = (System.nanoTime() - t0) / 1_000_000.0;
            STEP_LATENCY.labels(step, comp).observe(ms);  // ms statt Sekunden

            long cpu1 = THREAD_CPU_BEAN.getCurrentThreadCpuTime();
            if (cpu0 >= 0 && cpu1 >= cpu0) {
                double cpuMs = (cpu1 - cpu0) / 1_000_000.0;
                STEP_CPU.labels(step, comp).observe(cpuMs);
            }

            long usedHeap = MEM_BEAN.getHeapMemoryUsage().getUsed();
            STEP_HEAP_USED_BYTES.labels(step, comp).set(usedHeap);
        }

        public static void markError(String step, String comp) {
            STEP_ERRORS.labels(step, comp).inc();
        }
    }


    public static final class DocRun implements AutoCloseable {
        private final long t0 = System.nanoTime();
        private final long cpu0 = THREAD_CPU_BEAN.getCurrentThreadCpuTime();
        private final String threadName = Thread.currentThread().getName();
        private final String sizeBucket;
        private boolean error = false;

        DocRun(String sizeBucket) {
            this.sizeBucket = sizeBucket == null ? "unknown" : sizeBucket;
        }

        public void markError() { if (!error) { error = true; DOC_ERRORS.inc(); } }

        @Override public void close() {
            double ms = (System.nanoTime() - t0) / 1_000_000.0;
            DOC_TOTAL.labels(error ? "error" : "ok").observe(ms);

            WORKER_DOCS_PROCESSED.labels(threadName).inc();
            WORKER_DOC_LATENCY.labels(threadName).observe(ms);

            long cpu1 = THREAD_CPU_BEAN.getCurrentThreadCpuTime();
            if (cpu0 >= 0 && cpu1 >= cpu0) {
                double cpuMs = (cpu1 - cpu0) / 1_000_000.0;
                DOC_CPU_BY_SIZE.labels(sizeBucket).observe(cpuMs);
            }

            long usedHeap = MEM_BEAN.getHeapMemoryUsage().getUsed();
            DOC_HEAP_USED_BYTES.labels(error ? "error" : "ok", sizeBucket).set(usedHeap);
        }
    }

    // system sampling
    private void sampleSafely() {
        try {
            long now = System.currentTimeMillis();
            ts.add(now);

            // CPU Host
            long[] ticks = cpu.getSystemCpuLoadTicks();
            double sysLoad = cpu.getSystemCpuLoadBetweenTicks(prevTicks);
            prevTicks = ticks;
            measure("host_cpu_load_ratio", null, sysLoad);

            // CPU Prozess
            OSProcess current = os.getProcess(os.getProcessId());
            double procLoad = current.getProcessCpuLoadBetweenTicks(prevProc);
            prevProc = current;
            measure("process_cpu_load_ratio", null, procLoad);

            // Memory
            measure("host_memory_used_bytes", null, mem.getTotal() - mem.getAvailable());
            measure("process_resident_memory_bytes", null, current.getResidentSetSize());

            // Swap
            var vm = mem.getVirtualMemory();
            measure("host_swap_total_bytes", null, vm.getSwapTotal());
            measure("host_swap_used_bytes", null, vm.getSwapUsed());

            // Threads pro State (only for explicitly tracked threads)
            long[] ids = trackedThreadIdArray;
            if (ids != null && ids.length > 0) {
                ThreadInfo[] infos = tBean.getThreadInfo(ids, 0);
                java.util.Map<Thread.State,Integer> cnt = new java.util.EnumMap<>(Thread.State.class);
                for (ThreadInfo ti : infos) {
                    if (ti == null) continue;

                    cnt.merge(ti.getThreadState(), 1, Integer::sum);

                    long threadId = ti.getThreadId();
                    String threadName = ti.getThreadName();

                    // Per-thread CPU usage (ratio over sampling period)
                    long nowCpu = tBean.getThreadCpuTime(threadId);
                    if (nowCpu > 0L) {
                        Long prevCpu = prevThreadCpuNanos.get(threadId);
                        if (prevCpu != null && nowCpu >= prevCpu) {
                            double cpuRatio = (nowCpu - prevCpu) / (periodMillis * 1_000_000.0);
                            java.util.Map<String,String> labels = java.util.Map.of(
                                    "thread", threadName,
                                    "id", Long.toString(threadId)
                            );
                            measure("duui_worker_thread_cpu_ratio", labels, cpuRatio);

                            double idleRatio = Math.max(0.0, 1.0 - cpuRatio);
                            measure("duui_worker_thread_idle_ratio", labels, idleRatio);
                        }
                        prevThreadCpuNanos.put(threadId, nowCpu);
                    }

                    // Per-thread blocking behaviour
                    long blockedTime = ti.getBlockedTime();
                    if (blockedTime >= 0L) {
                        Long prevBlocked = prevBlockedTime.get(threadId);
                        if (prevBlocked != null && blockedTime >= prevBlocked) {
                            long delta = blockedTime - prevBlocked;
                            double blockedRatio = (double) delta / periodMillis;
                            measure(
                                "duui_worker_thread_blocked_ratio",
                                java.util.Map.of(
                                    "thread", threadName,
                                    "id", Long.toString(threadId)
                                ),
                                blockedRatio
                            );
                        }
                        prevBlockedTime.put(threadId, blockedTime);
                    }

                    // Per-thread waiting behaviour
                    long waitedTime = ti.getWaitedTime();
                    if (waitedTime >= 0L) {
                        Long prevWaited = prevWaitedTime.get(threadId);
                        if (prevWaited != null && waitedTime >= prevWaited) {
                            long delta = waitedTime - prevWaited;
                            double waitedRatio = (double) delta / periodMillis;
                            measure(
                                "duui_worker_thread_wait_ratio",
                                java.util.Map.of(
                                    "thread", threadName,
                                    "id", Long.toString(threadId)
                                ),
                                waitedRatio
                            );
                        }
                        prevWaitedTime.put(threadId, waitedTime);
                    }
                }
                for (Thread.State st : Thread.State.values()) {
                    measure("jvm_threads_state", java.util.Map.of("state", st.name()), cnt.getOrDefault(st, 0));
                }
            }

            // Network per interface (bytes/sec)
            for (NetworkIF nif : si.getHardware().getNetworkIFs()) {
                nif.updateAttributes();
                String ifName = nif.getName();
                long bytesRecv = nif.getBytesRecv();
                long bytesSent = nif.getBytesSent();

                Long prevRecv = prevNetBytesRecv.get(ifName);
                Long prevSent = prevNetBytesSent.get(ifName);
                if (prevRecv != null && prevSent != null) {
                    double seconds = periodMillis / 1000.0;
                    double recvPerSec = (bytesRecv - prevRecv) / seconds;
                    double sentPerSec = (bytesSent - prevSent) / seconds;
                    java.util.Map<String,String> lab = java.util.Map.of("if", ifName);
                    measure("net_bytes_recv_per_sec", lab, recvPerSec);
                    measure("net_bytes_sent_per_sec", lab, sentPerSec);
                }
                prevNetBytesRecv.put(ifName, bytesRecv);
                prevNetBytesSent.put(ifName, bytesSent);
            }

            // CPU interrupts / context switches (system-wide, per second)
            long interrupts = cpu.getInterrupts();
            long ctxSwitches = cpu.getContextSwitches();
            if (prevCpuInterrupts >= 0 && prevCpuCtxSwitches >= 0) {
                double seconds = periodMillis / 1000.0;
                double intrPerSec = (interrupts - prevCpuInterrupts) / seconds;
                double ctxPerSec = (ctxSwitches - prevCpuCtxSwitches) / seconds;
                measure("cpu_interrupts_per_sec", null, intrPerSec);
                measure("cpu_context_switches_per_sec", null, ctxPerSec);
            }
            prevCpuInterrupts = interrupts;
            prevCpuCtxSwitches = ctxSwitches;

            // GC metrics
            for (java.lang.management.GarbageCollectorMXBean gcBean
                    : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
                String gcName = gcBean.getName();
                long count = gcBean.getCollectionCount();
                long timeMs = gcBean.getCollectionTime();
                if (count < 0 || timeMs < 0) {
                    continue;
                }

                Long prevC = prevGcCount.get(gcName);
                Long prevT = prevGcTimeMs.get(gcName);
                if (prevC != null && prevT != null) {
                    long dCount = count - prevC;
                    long dTimeMs = timeMs - prevT;
                    double seconds = periodMillis / 1000.0;

                    if (dCount >= 0) {
                        double collectionsPerSec = dCount / seconds;
                        measure(
                                "jvm_gc_collections_per_sec",
                                java.util.Map.of("collector", gcName),
                                collectionsPerSec
                        );
                    }
                    if (dTimeMs >= 0) {
                        double gcTimeRatio = dTimeMs / (double) periodMillis;
                        measure(
                                "jvm_gc_time_ratio",
                                java.util.Map.of("collector", gcName),
                                gcTimeRatio
                        );
                    }
                }

                prevGcCount.put(gcName, count);
                prevGcTimeMs.put(gcName, timeMs);
            }
        } catch (Throwable ignore) {}
    }

    private void startProfiler() {
        try {
            String event = profileMode == ProfileMode.NO_CPU ? "itimer" : "cpu";
            String file = outputDirectory.resolve(runName + ".jfr").toString();
            String cmd = String.format(
                "start,jfr,event=%s,interval=10ms,wall=50ms,alloc=512k,lock=1ms,threads,file=%s",
                event,
                file
            );
            PROFILER.execute(cmd);
        } catch (Throwable ignore) {}
    }

    private void stopProfiler() {
        try {
            PROFILER.execute("stop");
        } catch (Throwable ignore) {}
    }

    private void writeSnapshotJson() throws Exception {
        ObjectMapper om = new ObjectMapper();
        ObjectNode root = om.createObjectNode();

        // Meta
        ObjectNode meta = root.putObject("meta");
        meta.put("runName", runName);
        meta.put("periodMillis", periodMillis);
        meta.put("points", ts.size());

        // Timeline
        ObjectNode timeline = root.putObject("timeline");
        ArrayNode timestamps = timeline.putArray("timestamps");
        for (Long t : ts) timestamps.add(t);

        ArrayNode metrics = timeline.putArray("series");
        for (var e : series.entrySet()) {
            String[] parts = e.getKey().split("\\|");
            String name = parts[0];
            ObjectNode s = metrics.addObject();
            s.put("name", name);
            ObjectNode lab = s.putObject("labels");
            for (int i=1;i<parts.length;i++) {
                String[] kv = parts[i].split("=", 2);
                if (kv.length==2) lab.put(kv[0], kv[1]);
            }
            ArrayNode vals = s.putArray("values");
            for (Double v : e.getValue()) vals.add(v);
        }

        // Histograms / selected metrics (Endstand)
        ObjectNode histos = root.putObject("histograms");
        Enumeration<Collector.MetricFamilySamples> mfs = CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (mfs.hasMoreElements()) {
            Collector.MetricFamilySamples fam = mfs.nextElement();
            for (Collector.MetricFamilySamples.Sample s : fam.samples) {
                boolean include =
                        s.name.startsWith("app_step_latency")
                        || s.name.startsWith("app_step_cpu")
                        || s.name.startsWith("app_document_total")
                        || s.name.startsWith("duui_worker_document_latency")
                        || s.name.startsWith("duui_document_cpu_by_size")
                        || s.name.startsWith("app_step_heap_used_bytes")
                        || s.name.startsWith("app_document_heap_used_bytes")
                        || s.name.startsWith("host_memory_total_bytes")
                        || s.name.startsWith("duui_worker_docs_processed_total");
                if (!include) continue;

                String base = s.name.replaceAll("(_bucket|_sum|_count)$", "");
                ObjectNode h = histos.withArray(base).addObject();
                ObjectNode lab = h.putObject("labels");
                for (int i=0;i<s.labelNames.size();i++) {
                    String ln = s.labelNames.get(i);
                    String lv = s.labelValues.get(i);
                    if (!"le".equals(ln)) lab.put(ln, lv);
                }
                if (fam.type == Collector.Type.HISTOGRAM || fam.type == Collector.Type.SUMMARY) {
                    if (s.name.endsWith("_bucket")) {
                        String le = null;
                        for (int i=0;i<s.labelNames.size();i++)
                            if ("le".equals(s.labelNames.get(i))) le = s.labelValues.get(i);
                        h.with("buckets").put(le==null?"+Inf":le, s.value);
                    } else if (s.name.endsWith("_sum")) {
                        h.put("sum", s.value);
                    } else if (s.name.endsWith("_count")) {
                        h.put("count", s.value);
                    }
                } else {
                    // Gauge/Counter: single value at end of run
                    h.put("value", s.value);
                }
            }
        }

        Path out = outputDirectory.resolve(runName + ".json");
        om.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), root);
    }



    // Beispiel
//    public static void main(String[] args) throws Exception {
//        AppMetrics m = new AppMetrics("run_2025_10_02");
//        m.start();
//        try (AppMetrics.DocRun doc = AppMetrics.docRun()) {
//            try (AppMetrics.Timer t = AppMetrics.timeStep("parse")) { Thread.sleep(200); }
//            try (AppMetrics.Timer t = AppMetrics.timeStep("validate","schema")) { Thread.sleep(120); }
//        }
//        new CountDownLatch(1).await(); // Prozess offen halten
//    }
}

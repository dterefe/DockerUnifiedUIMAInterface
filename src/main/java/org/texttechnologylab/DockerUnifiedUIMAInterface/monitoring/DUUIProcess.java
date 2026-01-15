package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;

/**
 * Thread-safe per-run state tracker fed by {@link DUUIEvent} stream.
 * <p>
 * The tracker processes events on a single event loop thread to avoid blocking the calling thread
 * (e.g. DUUIComposer) and to preserve ordering.
 */
public final class DUUIProcess implements IDUUIProcess {

    /**
     * Structured update emitted after applying an event to the in-memory snapshot.
     */
    public sealed interface Update permits Update.ProcessUpdate, Update.WorkerUpsert, Update.DriverUpsert,
            Update.ComponentUpsert, Update.InstanceUpsert, Update.DocumentUpsert {

        Meta meta();

        record Meta(String runKey, long eventId, long timestamp) {}

        record ProcessUpdate(Meta meta, Document process) implements Update {}

        record WorkerUpsert(Meta meta, String workerName, Document worker) implements Update {}

        record DriverUpsert(Meta meta, String driverName, Document driver) implements Update {}

        record ComponentUpsert(Meta meta, String componentId, Document component) implements Update {}

        record InstanceUpsert(Meta meta, String componentId, String instanceId, Document instance) implements Update {}

        record DocumentUpsert(Meta meta, String documentKey, Document document) implements Update {}
    }

    public static final class ProcessState extends DUUIState {
        private volatile String runId = "";
        private volatile int progress = 0;
        private volatile int total = 0;
        private volatile int skipped = 0;
        private volatile int initial = 0;
        private volatile boolean isFinished = false;
        private volatile long startedAt = 0L;
        private volatile long finishedAt = 0L;
        private volatile String error = "";
        private volatile boolean isTerminal = false;

        public boolean isTerminal() { return isTerminal; }

        public boolean isFinished() { return isFinished; }
        public long startedAt() { return startedAt; }
        public long finishedAt() { return finishedAt; }
        public int initial() { return initial; }
        public int skipped() { return skipped; }
        public int total() { return total; }
        public int progress() { return progress; }
        public String runId() { return runId; }
        public String error() { return error; }

        @Override
        public Document toDocument() {
            return super.toDocument()
                .append("runKey", runId)
                .append("progress", progress)
                .append("skipped", skipped)
                .append("initial", initial)
                .append("total", total)
                .append("started_at", startedAt)
                .append("finished_at", finishedAt)
                .append("is_finished", isFinished)
                .append("is_terminal", isTerminal)
                .append("error", error);
        }
    }

    public static final class WorkerState extends DUUIState {
        private final String name;
        private volatile int activeWorkers = 0;

        WorkerState(String name) { this.name = name; }

        public String getName() { return name; }

        @Override
        public Document toDocument() {
            return super.toDocument()
                .append("name", name)
                .append("activeWorkers", activeWorkers);
        }
    }

    public static final class ComponentState extends DUUIState {
        private final String componentId;
        private volatile String componentName = "";
        private volatile String driverName = "";
        private final Map<String, InstanceState> instancesById = new ConcurrentHashMap<>();
        private final AtomicLong activeInstances = new AtomicLong(0);
        private volatile boolean activatedOnce = false;

        ComponentState(String componentId) { this.componentId = componentId; }

        public String getComponentId() { return componentId; }
        public String getComponentName() { return componentName; }
        public String getDriverName() { return driverName; }
        public Map<String, InstanceState> getInstancesById() { return instancesById; }
        public long getActiveInstances() { return activeInstances.get(); }

        @Override
        public Document toDocument() {
            Document out = super.toDocument()
                .append("componentId", componentId)
                .append("componentName", componentName)
                .append("driverName", driverName)
                .append("activeInstances", activeInstances.get());

            Document instances = new Document();
            for (Map.Entry<String, InstanceState> e : instancesById.entrySet()) {
                instances.append(e.getKey(), e.getValue().toDocument());
            }
            out.append("instances", instances);
            return out;
        }
    }

    public static final class DriverState extends DUUIState {
        private final String driverName;
        private final AtomicLong activeInstances = new AtomicLong(0);
        private volatile boolean activatedOnce = false;

        DriverState(String driverName) {
            this.driverName = driverName;
            this.status = DUUIStatus.INACTIVE;
        }

        public String getDriverName() { return driverName; }
        public long getActiveInstances() { return activeInstances.get(); }

        @Override
        public Document toDocument() {
            return super.toDocument()
                .append("driverName", driverName)
                .append("activeInstances", activeInstances.get());
        }
    }

    public static final class InstanceState extends DUUIState {
        private final String instanceId;
        private volatile String endpoint = "";
        private final AtomicLong errorCount = new AtomicLong(0);
        private volatile DUUIContext.Payload lastErrorPayload;
        private volatile long lastErrorAt = 0L;
        private volatile boolean activatedOnce = false;

        InstanceState(String instanceId) {
            this.instanceId = instanceId;
            this.lastErrorPayload = DUUIContexts.defaultContext().payloadRecord();
        }

        public String getInstanceId() { return instanceId; }
        public String getEndpoint() { return endpoint; }
        public long getErrorCount() { return errorCount.get(); }
        public DUUIContext.Payload getLastErrorPayload() { return lastErrorPayload; }
        public long getLastErrorAt() { return lastErrorAt; }

        @Override
        public Document toDocument() {
            return super.toDocument()
                .append("instanceId", instanceId)
                .append("endpoint", endpoint)
                .append("errorCount", errorCount.get())
                .append("lastErrorPayload", DUUIContexts.toDocument(lastErrorPayload))
                .append("lastErrorAt", lastErrorAt);
        }
    }

    private final ProcessState process = new ProcessState();
    private final Map<String, DUUIDocument> documentsByPath = new ConcurrentHashMap<>();
    private final Map<String, Boolean> terminalDocumentsByPath = new ConcurrentHashMap<>();
    private final Map<String, String> pipelineStatus = new ConcurrentHashMap<>();
    private final Map<String, WorkerState> workersByName = new ConcurrentHashMap<>();
    private final Map<String, ComponentState> componentsById = new ConcurrentHashMap<>();
    private final Map<String, DriverState> driversByName = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<DUUIEvent> timeline = new ConcurrentLinkedQueue<>();
    private final Set<Long> processedEventIds = ConcurrentHashMap.newKeySet();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicReference<Thread> eventLoopThread = new AtomicReference<>();
    private final ExecutorService eventLoop = Executors.newSingleThreadExecutor(r -> {
        Thread t = Thread.ofVirtual().unstarted(r);
        eventLoopThread.set(t);
        return t;
    });

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    /**
     * State after reset. No more events are accepted.
     */
    private final AtomicBoolean isTerminal = new AtomicBoolean(false);

    private final AtomicBoolean acceptingEvents = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<Exception> fatalRunError = new AtomicReference<>(null);
    private final CopyOnWriteArrayList<Thread> workerThreads = new CopyOnWriteArrayList<>();

    private volatile DUUIProfiler appMetrics;

    /**
     * Process update observers (must be registered before run start).
     * Notified sequentially on the event loop thread.
     */
    private final CopyOnWriteArrayList<IDUUIProcessObserver> observers = new CopyOnWriteArrayList<>();

    @Override
    public void addObserver(IDUUIProcessObserver observer) {
        if (observer == null) return;
        if (started.get()) {
            throw new IllegalStateException("Observers must be registered before run start");
        }
        observers.addIfAbsent(observer);
    }

    @Override
    public void removeObserver(IDUUIProcessObserver observer) {
        if (observer == null) return;
        observers.remove(observer);
    }

    @Override
    public IDUUIProcess setProfiler(Path prometheusProfilerOutputPath, String runIdentifier) {
        if (prometheusProfilerOutputPath != null) {
            appMetrics = new DUUIProfiler(runIdentifier, prometheusProfilerOutputPath);
            appMetrics.start();

            Thread currentThread = Thread.currentThread();
            currentThread.setName("DUUIComposer-" + currentThread.getId());
            appMetrics.addThread(currentThread);
        }
        return this;
    }

    @Override
    public boolean hasProfiler() {
        return appMetrics != null;
    }

    @Override
    public void onEvent(DUUIEvent event) {
        // TODO: Refactor so eventLoop is non-blocking and observers receive events in-order 
        if (event == null || isShutdown.get() || isTerminal.get() || !acceptingEvents.get()) {
            return;
        }
        try {
            eventLoop.execute(() -> {
                if (isShutdown.get() || isTerminal.get()) return;

                List<Update> updates;
                lock.writeLock().lock();
                try {
                    if (isTerminal.get()) return;
                    if (!processedEventIds.add(event.getId())) return;
                    timeline.add(event);
                    updates = applyEvent(event);
                } finally {
                    lock.writeLock().unlock();
                }

                if (!updates.isEmpty() && !observers.isEmpty()) {
                    for (IDUUIProcessObserver obs : observers) {
                        try {
                            obs.onEvent(event, updates);
                        } catch (Throwable ignored) {
                        }
                    }
                }
            });
        } catch (RejectedExecutionException ignored) {
        }
    }

    /**
     * Stops internal executors and prevents further event processing.
     * Safe to call multiple times.
     */
    @Override
    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }
        acceptingEvents.set(false);
        observers.clear(); // eject ProcessUpdate observers
        eventLoop.shutdownNow();
        try {
            eventLoop.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        lock.writeLock().lock();
        try {
            timeline.clear();
            documentsByPath.clear();
            terminalDocumentsByPath.clear();
            pipelineStatus.clear();
            fatalRunError.set(null);
            workerThreads.clear();
            processedEventIds.clear();
            workersByName.clear();
            componentsById.clear();
            driversByName.clear();

            DUUIProfiler metrics = appMetrics;
            appMetrics = null;
            if (metrics != null) {
                try { metrics.close(); } catch (Exception ignored) {}
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void stopAcceptingEvents() {
        if (!acceptingEvents.compareAndSet(true, false)) {
            return;
        }
        if (isShutdown.get()) return;

        
        try {
            eventLoop.shutdown();
            eventLoop.awaitTermination(5, TimeUnit.MINUTES);
            Runnable terminalTask = () -> {
                DUUIEvent terminalEvent = null;
                Update.Meta meta = null;

                lock.writeLock().lock();
                try {
                    isTerminal.set(true);
                    process.isTerminal = true;
                    process.lastUpdatedAt = Math.max(process.lastUpdatedAt, System.currentTimeMillis());

                    terminalEvent = new DUUIEvent(
                        DUUIEvent.Sender.SYSTEM, 
                        "Process is terminal", 
                        process.lastUpdatedAt, 
                        DebugLevel.TRACE
                    );

                    timeline.add(terminalEvent);
                    processedEventIds.add(terminalEvent.getId());
                    process.lastEventId = terminalEvent.getId();

                    meta = new Update.Meta(
                        StringUtils.defaultString(process.runId),
                        terminalEvent.getId(),
                        terminalEvent.getTimestamp()
                    );
                } finally {
                    lock.writeLock().unlock();
                }

                if (!observers.isEmpty() && terminalEvent != null) {
                    for (IDUUIProcessObserver obs : observers) {
                        try {
                            obs.onEvent(terminalEvent, List.of(new Update.ProcessUpdate(meta, process.toDocument())));
                        } catch (Throwable ignored) {
                        }
                    }
                }
            };

            // If stopAcceptingEvents is called from the event loop itself, run terminal task inline to avoid deadlock.
            if (Thread.currentThread().equals(eventLoopThread.get())) {
                terminalTask.run();
            } else {
                eventLoop.execute(terminalTask);
            }
        } catch (RejectedExecutionException | InterruptedException ignored) {
            isTerminal.set(true);
            process.isTerminal = true;
        }

        observers.clear();
        eventLoop.shutdown();
        isShutdown.set(true);
    }

    public IDUUIProcess reset() {
        DUUIProcess newprocess = new DUUIProcess();
        
        for (IDUUIProcessObserver observer : observers) {
            newprocess.addObserver(observer);
        }

        stopAcceptingEvents();

        return newprocess;
    }

    @Override
    public boolean isTerminal() {
        return isTerminal.get();
    }

    @Override
    public boolean isFinished() {
        return process.isFinished;
    }

    @Override
    public int progress() {
        return process.progress;
    }

    @Override
    public List<DUUIEvent> drainEvents() {
        lock.writeLock().lock();
        List<DUUIEvent> drained = new ArrayList<>(timeline.size());
        try {
            DUUIEvent e;
            while ((e = timeline.poll()) != null) {
                drained.add(e);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return drained;
    }

    public List<DUUIEvent> eventsSnapshot() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(timeline);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DUUIDocument addDocument(DUUIDocument document) {
        if (document == null) return null;
        String path = document.getPath();
        if (StringUtils.isBlank(path)) return document;

        lock.writeLock().lock();
        try {
            DUUIDocument existing = documentsByPath.putIfAbsent(path, document);
            return existing != null ? existing : document;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void addDocuments(Collection<DUUIDocument> documents) {
        if (documents == null) return;
        for (DUUIDocument document : documents) {
            addDocument(document);
        }
    }

    @Override
    public Set<DUUIDocument> getDocuments() {
        lock.readLock().lock();
        try {
            return new HashSet<>(documentsByPath.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<String> getDocumentPaths() {
        lock.readLock().lock();
        try {
            return new HashSet<>(documentsByPath.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DUUIDocument findDocumentByPath(String path) {
        if (StringUtils.isBlank(path)) return null;
        lock.readLock().lock();
        try {
            return documentsByPath.get(path);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int getDocumentCount() {
        lock.readLock().lock();
        try {
            return documentsByPath.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clearDocuments() {
        lock.writeLock().lock();
        try {
            documentsByPath.clear();
            terminalDocumentsByPath.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Map<String, String> getPipelineStatus() {
        return pipelineStatus;
    }

    @Override
    public Map<String, DUUIStatus> getPipelineStatusEnum() {
        Map<String, DUUIStatus> mapped = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> entry : pipelineStatus.entrySet()) {
            mapped.put(entry.getKey(), DUUIStatus.fromString(entry.getValue()));
        }
        return mapped;
    }

    @Override
    public void setPipelineStatus(String name, DUUIStatus status) {
        if (StringUtils.isBlank(name) || status == null) return;
        pipelineStatus.put(name, status.toString());
    }

    @Override
    public void setPipelineStatus(String name, String status) {
        if (StringUtils.isBlank(name) || status == null) return;
        pipelineStatus.put(name, status);
    }

    @Override
    public Exception getFatalRunError() {
        return fatalRunError.get();
    }

    @Override
    public boolean setFatalRunErrorIfAbsent(Exception error) {
        Exception effective = error != null ? error : new RuntimeException("Worker failed");
        return fatalRunError.compareAndSet(null, effective);
    }

    @Override
    public void clearFatalRunError() {
        fatalRunError.set(null);
    }

    @Override
    public void registerWorker(Thread thread) {
        if (thread == null) return;
        workerThreads.addIfAbsent(thread);
    }

    @Override
    public List<Thread> workers() {
        return new ArrayList<>(workerThreads);
    }

    @Override
    public void clearWorkerThreads() {
        workerThreads.clear();
    }

    private List<Update> applyEvent(DUUIEvent event) {
        DUUIContext ctx = Objects.requireNonNullElse(event.getContext(), DUUIContexts.defaultContext());
        long ts = event.getTimestamp();
        long eventId = event.getId();

        String runKey = deriveRunKey(ctx);
        if (StringUtils.isBlank(process.runId) && StringUtils.isNotBlank(runKey)) {
            process.runId = runKey;
        }

        Update.Meta meta = new Update.Meta(StringUtils.defaultString(process.runId), eventId, ts);

        List<Update> out = new ArrayList<>(4);
        applyContext(event, ctx, meta, ts, eventId, out);
        return out;
    }

    private String deriveRunKey(DUUIContext ctx) {
        return switch (ctx) {
            case DUUIContext.ComposerContext c -> StringUtils.defaultString(c.runKey());
            case DUUIContext.WorkerContext w -> StringUtils.defaultString(w.composer().runKey());
            case DUUIContext.DocumentProcessContext p -> StringUtils.defaultString(p.composer().runKey());
            case DUUIContext.DocumentComponentProcessContext d -> StringUtils.defaultString(d.document().composer().runKey());
            default -> StringUtils.defaultString(process.runId);
        };
    }

    private void applyContext(DUUIEvent event, DUUIContext ctx, Update.Meta meta, long ts, long eventId, List<Update> out) {
        switch (ctx) {
            case DUUIContext.ComposerContext composerCtx -> {
                started.compareAndSet(false, true);

                if (StringUtils.isBlank(process.runId)) {
                    process.runId = composerCtx.runKey();
                }

                process.transitionStatus(composerCtx.status(), ts);
                process.lastUpdatedAt = ts;
                process.lastEventId = eventId;
                process.thread = ctx.thread();

                if (process.total == 0 && composerCtx.documentCount() > 0) {
                    process.total = composerCtx.documentCount();
                }

                if (process.startedAt == 0L && composerCtx.status() != DUUIStatus.UNKNOWN) {
                    process.startedAt = ts;
                }

                if (!process.isFinished
                    && (ctx.status() == DUUIStatus.COMPLETED
                    || ctx.status() == DUUIStatus.FAILED
                    || ctx.status() == DUUIStatus.CANCELLED)) {
                    process.isFinished = true;
                    process.finishedAt = ts;
                }

                if ((ctx.status() == DUUIStatus.FAILED || ctx.status() == DUUIStatus.CANCELLED)
                    && StringUtils.isBlank(process.error)) {
                    process.error = StringUtils.defaultString(event.getMessage());
                }

                out.add(new Update.ProcessUpdate(meta, process.toDocument()));
            }

            case DUUIContext.ReaderContext readerCtx  
            when readerCtx.status() == DUUIStatus.SETUP && readerCtx.reader() != null -> {
                process.transitionStatus(DUUIStatus.SETUP, ts);
                    process.lastUpdatedAt = ts;
                    process.lastEventId = eventId;
                    process.thread = ctx.thread();

                    if (process.total == 0) {
                        process.skipped = readerCtx.reader().getSkipped();
                        process.initial = readerCtx.reader().getInitial();
                        process.total = (int) readerCtx.reader().getSize();
                    }
                    if (process.startedAt == 0L) {
                        process.startedAt = ts;
                    }

                    out.add(new Update.ProcessUpdate(meta, process.toDocument()));
            }

            case DUUIContext.WorkerContext workerCtx -> {
                WorkerState worker = workersByName.computeIfAbsent(workerCtx.name(), WorkerState::new);

                worker.transitionStatus(workerCtx.status(), ts);
                worker.lastUpdatedAt = ts;
                worker.lastEventId = eventId;
                worker.thread = ctx.thread();
                worker.activeWorkers = workerCtx.activeWorkers().get();

                out.add(new Update.WorkerUpsert(meta, worker.getName(), worker.toDocument()));

                applyContext(event, workerCtx.composer(), meta, ts, eventId, out);
            }

            case DUUIContext.ComponentContext compCtx -> {
                ComponentState component = componentsById.computeIfAbsent(compCtx.componentId(), ComponentState::new);
                if (StringUtils.isBlank(component.componentName)) {
                    component.componentName = compCtx.componentName();
                }
                if (StringUtils.isBlank(component.driverName)) {
                    component.driverName = compCtx.driverName();
                }
                component.lastUpdatedAt = ts;
                component.lastEventId = eventId;
                component.thread = ctx.thread();

                DUUIStatus compStatus = compCtx.status();
                if (compStatus != DUUIStatus.INSTANTIATING && compStatus != DUUIStatus.SHUTDOWN) {
                    if (component.activeInstances.get() > 0) {
                        compStatus = DUUIStatus.ACTIVE;
                    } else if (component.activatedOnce) {
                        compStatus = DUUIStatus.IDLE;
                    } else {
                        compStatus = DUUIStatus.INACTIVE;
                    }
                }
                component.transitionStatus(compStatus, ts);

                deriveDriverStatus(component, ts, eventId);

                out.add(new Update.ComponentUpsert(meta, component.componentId, component.toDocument()));
                out.add(new Update.DriverUpsert(meta, component.driverName, driversByName.get(component.driverName).toDocument()));
            }

            case DUUIContext.InstantiatedComponentContext instCtx -> {
                DUUIContext.ComponentContext compCtx = instCtx.component();

                ComponentState component = componentsById.computeIfAbsent(compCtx.componentId(), ComponentState::new);
                if (StringUtils.isBlank(component.componentName)) component.componentName = compCtx.componentName();
                if (StringUtils.isBlank(component.driverName)) component.driverName = compCtx.driverName();

                InstanceState instance = component.instancesById.computeIfAbsent(instCtx.instanceId(), InstanceState::new);
                if (StringUtils.isBlank(instance.endpoint)) instance.endpoint = instCtx.endpoint();

                DUUIStatus next = instCtx.status();
                if (next == DUUIStatus.FAILED) {
                    instance.errorCount.incrementAndGet();
                    instance.lastErrorPayload = instCtx.payloadRecord();
                    instance.lastErrorAt = ts;
                    next = DUUIStatus.IDLE;
                }

                boolean wasActive = isEffectivelyActive(instance.status);
                boolean willBeActive = isEffectivelyActive(next);
                if (wasActive && !willBeActive) {
                    component.activeInstances.updateAndGet(prev -> Math.max(0L, prev - 1L));
                } else if (!wasActive && willBeActive) {
                    component.activeInstances.incrementAndGet();
                }

                if (willBeActive && !instance.activatedOnce) {
                    instance.activatedOnce = true;
                    component.activatedOnce = true;
                }

                instance.transitionStatus(next, ts);
                instance.lastUpdatedAt = ts;
                instance.lastEventId = eventId;
                instance.thread = ctx.thread();

                applyContext(event, compCtx, meta, ts, eventId, out);

                out.add(new Update.InstanceUpsert(meta, component.componentId, instance.instanceId, instance.toDocument()));
            }

            case DUUIContext.DocumentContext documentCtx -> {
                DUUIDocument document = documentCtx.document();

                document.getState().update(event, documentCtx);
                documentsByPath.put(document.getPath(), document);
                DUUIStatus docStatus = documentCtx.status();
                if (docStatus == DUUIStatus.COMPLETED || docStatus == DUUIStatus.FAILED || docStatus == DUUIStatus.CANCELLED) {
                    String key = document.getPath();
                    if (StringUtils.isNotBlank(key) && terminalDocumentsByPath.putIfAbsent(key, Boolean.TRUE) == null) {
                        process.progress = Math.max(0, process.progress + 1);
                    }
                }
                out.add(new Update.DocumentUpsert(meta, document.getPath(), document.getState().toDocument()));
            }

            case DUUIContext.DocumentProcessContext procCtx -> {
                applyContext(event, procCtx.composer(), meta, ts, eventId, out);
                applyContext(event, procCtx.document(), meta, ts, eventId, out);
            }

            case DUUIContext.ReaderDocumentContext readerDocCtx -> {
                applyContext(event, readerDocCtx.reader(), meta, ts, eventId, out);
                applyContext(event, readerDocCtx.document(), meta, ts, eventId, out);
            }

            case DUUIContext.DocumentComponentProcessContext dcpCtx -> {
                applyContext(event, dcpCtx.document(), meta, ts, eventId, out);
                applyContext(event, dcpCtx.component(), meta, ts, eventId, out);
            }

            default -> {
            }
        }
    }

    private void deriveDriverStatus(ComponentState component, long ts, long eventId) {
        String driverName = component.driverName;
        DriverState driver = driversByName.computeIfAbsent(driverName, DriverState::new);

        long active = 0;
        for (ComponentState cs : componentsById.values()) {
            if (driverName.equals(cs.driverName)) {
                active += Math.max(0, cs.activeInstances.get());
            }
        }
        driver.activeInstances.set(active);
        if (active > 0) {
            driver.activatedOnce = true;
            driver.transitionStatus(DUUIStatus.ACTIVE, ts);
        } else if (driver.activatedOnce || component.activatedOnce) {
            driver.activatedOnce = true;
            driver.transitionStatus(DUUIStatus.IDLE, ts);
        } else {
            driver.transitionStatus(DUUIStatus.INACTIVE, ts);
        }
        driver.lastUpdatedAt = ts;
        driver.lastEventId = eventId;
        driver.thread = component.thread;
    }

    public static boolean isInstancePhase(DUUIStatus status) {
        return status == DUUIStatus.COMPONENT_WAIT
            || status == DUUIStatus.COMPONENT_SERIALIZE
            || status == DUUIStatus.COMPONENT_PROCESS
            || status == DUUIStatus.COMPONENT_DESERIALIZE
            || status == DUUIStatus.COMPONENT_LUA_PROCESS;
    }

    public static boolean isEffectivelyActive(DUUIStatus status) {
        if (status == DUUIStatus.ACTIVE) return true;
        return isInstancePhase(status);
    }

    public Document toDocument() {
        lock.readLock().lock();
        try {
            Document out = process.toDocument();

            Document workers = new Document();
            for (Map.Entry<String, WorkerState> e : workersByName.entrySet()) {
                workers.append(e.getKey(), e.getValue().toDocument());
            }
            out.append("workers", workers);

            Document components = new Document();
            for (Map.Entry<String, ComponentState> e : componentsById.entrySet()) {
                components.append(e.getKey(), e.getValue().toDocument());
            }
            out.append("components", components);

            Document drivers = new Document();
            for (Map.Entry<String, DriverState> e : driversByName.entrySet()) {
                drivers.append(e.getKey(), e.getValue().toDocument());
            }
            out.append("drivers", drivers);

            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "DUUIProcess(runKey=" + Objects.toString(process.runId, "") + ", status=" + process.status + ")";
    }
}

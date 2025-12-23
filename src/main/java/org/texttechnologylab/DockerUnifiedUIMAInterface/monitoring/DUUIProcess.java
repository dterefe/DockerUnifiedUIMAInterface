package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.PayloadKind;

/**
 * Thread-safe per-run state tracker fed by {@link DUUIEvent} stream.
 * <p>
 * The run status is derived exclusively from {@link DUUIContext.ComposerContext}.
 * All other context statuses are scoped (document/component/worker/driver) and
 * update only their respective state.
 */
public final class DUUIProcess implements AutoCloseable {

    public static final class RunState extends DUUIState {
        private volatile String runKey;

        protected volatile AtomicInteger progress = new AtomicInteger(0);
        protected volatile long total = 0;
        protected volatile int skipped = 0;
        protected volatile int initial = 0;
        protected volatile boolean isFinished = false;
        protected volatile long startedAt = 0L;
        protected volatile long finishedAt = 0L;
        protected volatile String error = "";

        public boolean isFinished() { return isFinished; }

        public long startedAt() { return startedAt; }

        public long finishedAt() { return finishedAt; }

        public int initial() { return initial; }

        public int skipped() { return skipped; }

        public long total() { return total; }

        public int progress() { return progress.get(); }

        public String runKey() { return runKey; }

        public Document toDocument() {
            return super.toDocument()
                .append("runKey", runKey)
                .append("progress", progress.get())
                .append("skipped", skipped)
                .append("initial", initial)
                .append("total", total)
                .append("started_at", startedAt)
                .append("finished_at", finishedAt)
                .append("is_finished", isFinished)
                .append("error", error);
        }
    }

    public static final class WorkerState extends DUUIState {
        private final String name;

        WorkerState(String name) { this.name = name; }

        public String getName() { return name; }

        @Override
        public Document toDocument() {
            return super.toDocument().append("name", name);
        }
    }

    public static final class ComponentState extends DUUIState {
        private final String componentId;
        private volatile String componentName;
        private volatile String driverName;
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
        private volatile String endpoint;
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

    private final RunState run = new RunState();
    private final Map<String, DUUIDocument> documentsByPath = new ConcurrentHashMap<>();
    private final Map<String, WorkerState> workersByName = new ConcurrentHashMap<>();
    private final Map<String, ComponentState> componentsById = new ConcurrentHashMap<>();
    private final Map<String, DriverState> driversByName = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<DUUIEvent> timeline = new ConcurrentLinkedQueue<>();
    private final Map<Long, DUUIEvent> eventsById = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService virtualThreadPool = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService eventLoop = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Observers notified asynchronously whenever a new {@link DUUIEvent} is processed.
     * Intended for real-time web UI updates (e.g. websockets).
     */
    private final CopyOnWriteArrayList<DUUIEventObserver> eventObservers = new CopyOnWriteArrayList<>();

    public RunState run() { return run; }

    public void addEventObserver(DUUIEventObserver observer) {
        eventObservers.add(observer);
    }

    public void removeEventObserver(DUUIEventObserver observer) {
        eventObservers.remove(observer);
    }

    public void onEvent(DUUIEvent event) {
        if (event == null || isShutdown.get()) {
            return;
        }
        eventLoop.execute(() -> {
            DUUIContext ctx = event.getContext();

            lock.writeLock().lock();
            try {
                timeline.add(event);
                eventsById.put(event.getId(), event);
                onContext(event, ctx);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    /**
     * Stops internal executors and prevents further event processing.
     * Safe to call multiple times.
     */
    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        eventLoop.shutdownNow();
        virtualThreadPool.shutdown();

        try {
            eventLoop.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            virtualThreadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Drains and returns queued events in insertion order.
     */
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

    /**
     * Returns a snapshot of currently retained events (does not clear).
     */
    public List<DUUIEvent> eventsSnapshot() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(timeline);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<DUUIEvent> eventById(long eventId) {
        if (eventId <= 0L) return Optional.empty();
        return Optional.ofNullable(eventsById.get(eventId));
    }


    private void onContext(DUUIEvent event, DUUIContext ctx) {
        switch (ctx) {
            case DUUIContext.ComposerContext composerCtx -> {
                run.transitionStatus(composerCtx.status(), event.getTimestamp());
                run.lastUpdatedAt = event.getTimestamp();
                run.lastEventId = event.getId();
                run.thread = ctx.thread();
                
                run.runKey = composerCtx.runKey();
                run.progress = composerCtx.progressAtomic();
                run.total = composerCtx.total();
                
                if (run.startedAt == 0L && composerCtx.status() != DUUIStatus.UNKNOWN) 
                    run.startedAt = event.getTimestamp();

                if (!run.isFinished && 
                    (ctx.status() == DUUIStatus.COMPLETED 
                    || ctx.status() == DUUIStatus.FAILED 
                    || ctx.status() == DUUIStatus.CANCELLED)) { 
                    run.isFinished = true; 
                    run.finishedAt = event.getTimestamp(); 
                }

                if (ctx.payloadKind() == PayloadKind.STACKTRACE) {
                    run.error = event.getMessage();
                }

                notifyEventObservers(event, run);
            }

            case DUUIContext.WorkerContext workerCtx -> {
                WorkerState worker = workersByName.computeIfAbsent(workerCtx.name(), WorkerState::new);

                worker.transitionStatus(workerCtx.status(), event.getTimestamp());
                worker.lastUpdatedAt = event.getTimestamp();
                worker.lastEventId = event.getId();
                worker.thread = ctx.thread();
                
                onContext(event, workerCtx.composer());

                notifyEventObservers(event, worker);
            }

            case DUUIContext.DocumentContext documentCtx -> {
                DUUIDocument document = documentCtx.document();
                document.getState().update(event, documentCtx);
                documentsByPath.put(document.getPath(), document);

                notifyEventObservers(event, document);
            }

            case DUUIContext.ComponentContext compCtx -> {
                ComponentState component = componentsById.computeIfAbsent(compCtx.componentId(), ComponentState::new);
                component.componentName = compCtx.componentName();
                component.driverName = compCtx.driverName();
                component.lastUpdatedAt = event.getTimestamp();
                component.lastEventId = event.getId();
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

                component.transitionStatus(compStatus, event.getTimestamp());

                deriveDriverStatus(component, event.getTimestamp(), event.getId());
                
                notifyEventObservers(event, component);
            }

            case DUUIContext.InstantiatedComponentContext instCtx -> {
                DUUIContext.ComponentContext compCtx = instCtx.component();

                ComponentState component = componentsById.computeIfAbsent(compCtx.componentId(), ComponentState::new);

                InstanceState instance = component.instancesById.computeIfAbsent(instCtx.instanceId(), InstanceState::new);
                instance.endpoint = instCtx.endpoint();

                DUUIStatus next = instCtx.status();
                if (next == DUUIStatus.FAILED) {
                    instance.errorCount.incrementAndGet();
                    instance.lastErrorPayload = instCtx.payloadRecord();
                    instance.lastErrorAt = event.getTimestamp();
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

                instance.transitionStatus(next, event.getTimestamp());
                instance.lastUpdatedAt = event.getTimestamp();
                instance.lastEventId = event.getId();
                instance.thread = ctx.thread();

                onContext(event, compCtx);
            }

            case DUUIContext.DocumentProcessContext procCtx -> {
                onContext(event, procCtx.composer());
                onContext(event, procCtx.document());
            }

            case DUUIContext.DocumentComponentProcessContext dcpCtx -> {
                onContext(event, dcpCtx.document());
                onContext(event, dcpCtx.component());

                DUUIDocument doc = dcpCtx.document().document().document();

                if (dcpCtx.status() == DUUIStatus.FAILED) {
                    String componentId = dcpCtx.component().component().componentId();
                    DUUIContext.Payload payload = dcpCtx.payloadRecord();
                    doc.setComponentErrorPayload(componentId, payload);
                }

                DUUIContext.Payload payload = dcpCtx.payloadRecord();
                if (payload.kind() == DUUIContext.PayloadKind.METRIC_MILLIS) {
                    long millis;
                    try {
                        millis = Long.parseLong(payload.content());
                    } catch (NumberFormatException ignored) {
                        break;
                    }
                    DUUIStatus phase = dcpCtx.status();
                    String componentId = dcpCtx.component().component().componentId();
                    doc.addComponentPhaseDurationMillis(componentId, phase, millis);
                }
            }

            case DUUIContext.ReaderContext readerCtx -> {
                if (readerCtx.status() == DUUIStatus.SETUP) {
                    run.transitionStatus(DUUIStatus.SETUP, event.getTimestamp());
                    run.lastUpdatedAt = event.getTimestamp();
                    run.skipped = readerCtx.reader().getSkipped();
                    run.initial = readerCtx.reader().getInitial();
                    run.total = readerCtx.reader().getSize();
                    run.thread = ctx.thread();
                    run.lastEventId = event.getId();

                    notifyEventObservers(event, run);
                }
            }

            case DUUIContext.ReaderDocumentContext readerDocCtx -> {
                onContext(event, readerDocCtx.reader());
                onContext(event, readerDocCtx.document());
            }

            default -> {
            }
        }
    }

    private void notifyEventObservers(DUUIEvent event, DUUIState newState) {
        if (eventObservers.isEmpty()) {
            return;
        }
        for (DUUIEventObserver observer : eventObservers) {
            CompletableFuture.runAsync(() -> {
                try {
                    observer.onEvent(event, newState);
                } catch (Throwable ignored) {
                }
            }, virtualThreadPool);
        }
    }

    private void notifyEventObservers(DUUIEvent event, DUUIDocument newState) {
        if (eventObservers.isEmpty()) {
            return;
        }
        for (DUUIEventObserver observer : eventObservers) {
            CompletableFuture.runAsync(() -> {
                try {
                    observer.onEvent(event, newState);
                } catch (Throwable ignored) {
                }
            }, virtualThreadPool);
        }
    }

    private void deriveDriverStatus(ComponentState component, long ts, long eventId) {
        String driverName = component.driverName;
        DriverState driver = driversByName.computeIfAbsent(driverName, DriverState::new);

        // Recompute driver active instances as sum of its components.
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
            Document out = run.toDocument();

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
        return "DUUIProcess(runKey=" + Objects.toString(run.runKey, "") + ", status=" + run.status + ")";
    }
}

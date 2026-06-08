package org.texttechnologylab.duui.orchestration.worker;

import org.texttechnologylab.duui.DUUIWorkerContext;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.DUUIFrameworkStateException;
import org.texttechnologylab.duui.orchestration.DUUITask;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DUUIWorker implements DUUIActor {

    /**
     * Thread environment classification.
     * [DESIGN: lines 36-37]
     */
    public enum Environment {
        PLATFORM,
        VIRTUAL
    }

    public enum Type {
        ORCHESTRATOR,
        PIPELINE,
        SERVICE
    }

    /**
     * ThreadFactory implementation creating DUUIWorker objects.
     * Accumulates counters per DUUIWorker.Type + DUUIWorker.Environment.
     * Context propagation: captures parent worker context → assigns to new thread.
     * [DESIGN: lines 49-57]
     */
    public static final class Factory implements ThreadFactory {

        private final String orchestratorId;
        private final Environment environment;
        private final Type type;
        private final AtomicInteger counter;

        public Factory(String orchestratorId, Environment environment, Type type) {
            this.orchestratorId = orchestratorId;
            this.environment = environment == null ? Environment.PLATFORM : environment;
            this.type = type == null ? Type.PIPELINE : type;
            this.counter = new AtomicInteger();
        }

        public static Factory platform(String orchestratorId, Type type) {
            return new Factory(orchestratorId, Environment.PLATFORM, type);
        }

        public static Factory virtual(String orchestratorId, Type type) {
            return new Factory(orchestratorId, Environment.VIRTUAL, type);
        }

        public Environment environment() { return environment; }
        public Type type() { return type; }
        public int count() { return counter.get(); }

        @Override
        public Thread newThread(Runnable r) {
            DUUIWorkerContext captured = captureParentContext();

            Runnable wrapped = () -> {
                DUUIWorkerRegistry.registerCurrentThread(orchestratorId, environment, type);
                DUUIWorkerContext.bind(DUUIWorker.current().getWorkerContext());
                if (captured != null) {
                    DUUIWorkerContext.current().copyFrom(captured);
                }
                try {
                    r.run();
                } finally {
                    if (environment == Environment.VIRTUAL) {
                        DUUIWorkerRegistry.unregisterCurrentThread();
                    }
                }
            };

            counter.incrementAndGet();

            if (environment == Environment.VIRTUAL) {
                return Thread.ofVirtual()
                        .name("duui-" + type.name().toLowerCase() + "-virtual-" + counter.get())
                        .factory()
                        .newThread(wrapped);
            } else {
                return Thread.ofPlatform()
                        .name("duui-" + type.name().toLowerCase() + "-platform-" + counter.get())
                        .factory()
                        .newThread(wrapped);
            }
        }

        private static DUUIWorkerContext captureParentContext() {
            try {
                return DUUIWorkerContext.current().copy();
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    private final String id;
    private volatile String orchestratorId;
    private final long threadId;
    private final Environment environment;
    private final Type type;
    private volatile DUUITask<?> currentTask;
    private final DUUIWorkerContext context;

    DUUIWorker(String orchestratorId, long threadId, Environment environment, Type type) {
        this.id = UUID.randomUUID().toString();
        this.orchestratorId = Objects.requireNonNull(orchestratorId, "orchestratorId");
        this.threadId = threadId;
        this.environment = environment == null ? Environment.PLATFORM : environment;
        this.type = type == null ? Type.PIPELINE : type;
        this.context = new DUUIWorkerContext();
    }

    public static DUUIWorker current() {
        return DUUIWorkerRegistry.currentWorker()
                .orElseThrow(() -> new DUUIFrameworkStateException("No DUUIWorker is registered for thread " + Thread.currentThread().threadId()));
    }
    /**
     * Returns this worker's context (instance accessor).
     * Use {@link #context()} for the static convenience form.
     */
    DUUIWorkerContext getWorkerContext() {
        return context;
    }

    /**
     * Static accessor for the current worker's context.
     * [DESIGN: line 44] — {@code DUUIWorker.context()} equals {@code DUUIWorker.current().getWorkerContext()}
     */
    public static DUUIWorkerContext context() {
        return current().getWorkerContext();
    }

    public DUUITask<?> requireCurrentTask() {
        DUUITask<?> task = currentTask;
        if (task == null) {
            throw new DUUIFrameworkStateException("No DUUITask is bound to worker " + id);
        }
        return task;
    }

    public void bind(DUUITask<?> task) {
        Objects.requireNonNull(task, "task");
        if (!orchestratorId.equals(task.orchestratorId())) {
            throw new DUUIFrameworkStateException("Cannot bind task from orchestrator " + task.orchestratorId() + " to worker from orchestrator " + orchestratorId);
        }
        this.currentTask = task;
        DUUIEventService.bindCurrent(task.context().eventService(), task.context().eventContext());
    }

    public void clear(DUUITask<?> task) {
        if (currentTask == task) {
            currentTask = null;
            DUUIEventService.clearCurrent();
        }
    }

    void assignOrchestrator(String orchestratorId) {
        if (currentTask != null) {
            throw new DUUIFrameworkStateException("Cannot reassign worker " + id + " while task " + currentTask.id() + " is active.");
        }
        this.orchestratorId = Objects.requireNonNull(orchestratorId, "orchestratorId");
    }

    public String id() { return id; }
    public String orchestratorId() { return orchestratorId; }
    public long threadId() { return threadId; }
    public Environment environment() { return environment; }
    public Type type() { return type; }
    public DUUITask<?> currentTask() { return currentTask; }

}

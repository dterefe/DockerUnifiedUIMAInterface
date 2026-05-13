package org.texttechnologylab.duui.orchestration;

import java.util.Objects;
import java.util.UUID;

public final class DUUIWorker {
    private final String id;
    private volatile String orchestratorId;
    private final long threadId;
    private final DUUIWorkerKind kind;
    private final boolean originThread;
    private volatile DUUITask<?> currentTask;

    DUUIWorker(String orchestratorId, long threadId, DUUIWorkerKind kind, boolean originThread) {
        this.id = UUID.randomUUID().toString();
        this.orchestratorId = Objects.requireNonNull(orchestratorId, "orchestratorId");
        this.threadId = threadId;
        this.kind = kind == null ? DUUIWorkerKind.PLATFORM : kind;
        this.originThread = originThread;
    }

    public static DUUIWorker current() {
        return DUUIWorkerRegistry.currentWorker()
                .orElseThrow(() -> new DUUIFrameworkStateException("No DUUIWorker is registered for thread " + Thread.currentThread().threadId()));
    }

    public DUUITask<?> requireCurrentTask() {
        DUUITask<?> task = currentTask;
        if (task == null) {
            throw new DUUIFrameworkStateException("No DUUITask is bound to worker " + id);
        }
        return task;
    }

    void bind(DUITaskBinding binding) {
        DUUITask<?> task = binding.task();
        if (!orchestratorId.equals(task.orchestratorId())) {
            throw new DUUIFrameworkStateException("Cannot bind task from orchestrator " + task.orchestratorId() + " to worker from orchestrator " + orchestratorId);
        }
        this.currentTask = task;
    }

    void clear(DUITaskBinding binding) {
        if (currentTask == binding.task()) {
            currentTask = null;
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
    public DUUIWorkerKind kind() { return kind; }
    public boolean originThread() { return originThread; }
    public DUUITask<?> currentTask() { return currentTask; }

    record DUITaskBinding(DUUITask<?> task) {
        DUITaskBinding {
            Objects.requireNonNull(task, "task");
        }
    }
}

package org.texttechnologylab.duui.orchestration;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class DUUIWorkerRegistry {
    private static final Map<Long, DUUIWorker> WORKERS_BY_THREAD_ID = new ConcurrentHashMap<>();

    private DUUIWorkerRegistry() {}

    public static DUUIWorker registerCurrentThread(String orchestratorId, DUUIWorkerKind kind, boolean originThread) {
        long threadId = Thread.currentThread().threadId();
        return WORKERS_BY_THREAD_ID.compute(threadId, (ignored, existing) -> {
            if (existing != null && !existing.orchestratorId().equals(orchestratorId)) {
                existing.assignOrchestrator(orchestratorId);
                return existing;
            }
            if (existing != null) return existing;
            return new DUUIWorker(orchestratorId, threadId, kind, originThread);
        });
    }

    public static Optional<DUUIWorker> currentWorker() {
        return Optional.ofNullable(WORKERS_BY_THREAD_ID.get(Thread.currentThread().threadId()));
    }

    public static void unregisterCurrentThread() {
        WORKERS_BY_THREAD_ID.remove(Thread.currentThread().threadId());
    }
}

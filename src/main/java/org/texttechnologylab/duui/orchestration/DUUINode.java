package org.texttechnologylab.duui.orchestration;

import java.time.Instant;
import java.util.Objects;

public final class DUUINode {
    private final String id;
    private final DUUIRuntimeKind runtimeKind;
    private final String endpoint;
    private final int capacity;
    private volatile boolean healthy = true;
    private volatile int currentLoad;
    private volatile Instant lastFailureAt;

    public DUUINode(String id, DUUIRuntimeKind runtimeKind, String endpoint, int capacity) {
        this.id = Objects.requireNonNull(id, "id");
        this.runtimeKind = runtimeKind == null ? DUUIRuntimeKind.LOCAL_JVM : runtimeKind;
        this.endpoint = endpoint;
        this.capacity = Math.max(1, capacity);
    }

    public String id() { return id; }
    public DUUIRuntimeKind runtimeKind() { return runtimeKind; }
    public String endpoint() { return endpoint; }
    public int capacity() { return capacity; }
    public boolean healthy() { return healthy; }
    public int currentLoad() { return currentLoad; }
    public Instant lastFailureAt() { return lastFailureAt; }

    public boolean hasCapacity() { return healthy && currentLoad < capacity; }
    public void acquire() { currentLoad++; }
    public void release() { currentLoad = Math.max(0, currentLoad - 1); }
    public void markHealthy() { healthy = true; }
    public void markFailed() { healthy = false; lastFailureAt = Instant.now(); }
}

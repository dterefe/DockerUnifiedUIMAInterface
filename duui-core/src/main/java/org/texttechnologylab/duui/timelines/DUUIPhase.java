package org.texttechnologylab.duui.timelines;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;

public final class DUUIPhase {
    private final String id;
    private final DUUIStatus status;
    private final DUUIDispatchMode dispatchMode;
    private final Method method;
    private final String parent;
    private final Instant createdAt;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;
    private volatile DUUILifecycle lifecycle;
    private volatile Throwable failure;

    DUUIPhase(DUUIStatus status, DUUIDispatchMode dispatchMode, Method method, String parent) {
        this.id = UUID.randomUUID().toString();
        this.status = Objects.requireNonNull(status, "status");
        this.dispatchMode = dispatchMode == null ? DUUIDispatchMode.MIXED : dispatchMode;
        this.method = Objects.requireNonNull(method, "method");
        this.parent = parent;
        this.createdAt = Instant.now();
        this.lifecycle = DUUILifecycle.CREATION;
    }

    public String id() {
        return id;
    }

    public String name() {
        return status.name().toLowerCase();
    }

    public Method method() {
        return method;
    }

    public Optional<String> parent() {
        return Optional.ofNullable(parent);
    }

    public DUUIStatus status() {
        return status;
    }

    public DUUIDispatchMode dispatchMode() {
        return dispatchMode;
    }

    public DUUILifecycle lifecycle() {
        return lifecycle;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Optional<Instant> startedAt() {
        return Optional.ofNullable(startedAt);
    }

    public Optional<Instant> finishedAt() {
        return Optional.ofNullable(finishedAt);
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    public Optional<Duration> duration() {
        Instant start = startedAt;
        Instant finish = finishedAt;
        if (start == null || finish == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.between(start, finish));
    }

    void start() {
        startedAt = Instant.now();
        lifecycle = lifecycle.transitionTo(DUUILifecycle.ACTIVE);
    }

    void finish() {
        finishedAt = Instant.now();
        lifecycle = lifecycle.transitionTo(DUUILifecycle.TERMINAL);
    }

    void fail(Throwable throwable) {
        failure = throwable;
        finishedAt = Instant.now();
        lifecycle = lifecycle.transitionTo(DUUILifecycle.TERMINAL);
    }

    void cancel() {
        finishedAt = Instant.now();
        lifecycle = lifecycle.transitionTo(DUUILifecycle.TERMINAL);
    }
}

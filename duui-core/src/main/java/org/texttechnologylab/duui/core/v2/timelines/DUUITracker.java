package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.storage.DUUIInMemoryIndex;
import org.texttechnologylab.duui.storage.DUUIInMemoryRegistry;
import org.texttechnologylab.duui.storage.DUUIIndex;
import org.texttechnologylab.duui.storage.DUUIRegistry;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class DUUITracker {
    private final DUUIIndex<DUUIStatus, String> started;
    private final DUUIIndex<DUUIStatus, String> finished;
    private final DUUIIndex<DUUIStatus, String> failed;
    private final DUUIRegistry<String, Throwable> failures;

    public DUUITracker() {
        this(new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryRegistry<>());
    }

    public DUUITracker(
            DUUIIndex<DUUIStatus, String> started,
            DUUIIndex<DUUIStatus, String> finished,
            DUUIIndex<DUUIStatus, String> failed,
            DUUIRegistry<String, Throwable> failures
    ) {
        this.started = Objects.requireNonNull(started, "started");
        this.finished = Objects.requireNonNull(finished, "finished");
        this.failed = Objects.requireNonNull(failed, "failed");
        this.failures = Objects.requireNonNull(failures, "failures");
    }

    public void start(DUUIPhase phase) {
        started.add(phase.status(), phase.id());
    }

    public void finish(DUUIPhase phase) {
        finished.add(phase.status(), phase.id());
    }

    public void fail(DUUIPhase phase, Throwable throwable) {
        failed.add(phase.status(), phase.id());
        failures.put(phase.id(), throwable);
    }

    public Stream<String> started(DUUIStatus status) {
        return started.find(status).stream();
    }

    public Stream<String> finished(DUUIStatus status) {
        return finished.find(status).stream();
    }

    public Stream<String> failed(DUUIStatus status) {
        return failed.find(status).stream();
    }

    public Optional<Throwable> failure(String phase) {
        return failures.get(phase);
    }
}

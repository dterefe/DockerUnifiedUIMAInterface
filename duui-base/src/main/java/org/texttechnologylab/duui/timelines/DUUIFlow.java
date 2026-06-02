package org.texttechnologylab.duui.timelines;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class DUUIFlow<T> {
    private final CompletableFuture<T> completion;
    private final CopyOnWriteArrayList<Runnable> dispatchHooks;
    private final CopyOnWriteArrayList<Consumer<? super T>> completionHooks;
    private final CopyOnWriteArrayList<Consumer<? super Throwable>> failureHooks;
    private final CopyOnWriteArrayList<Consumer<? super Throwable>> cancellationHooks;
    private final CopyOnWriteArrayList<DUUITracker> trackers;
    private final TimerTracker timer;
    private volatile boolean dispatched;
    private volatile boolean done;
    private volatile Throwable cancellationCause;
    private volatile DUUIPhase phase;

    private DUUIFlow() {
        this.completion = new CompletableFuture<>();
        this.dispatchHooks = new CopyOnWriteArrayList<>();
        this.completionHooks = new CopyOnWriteArrayList<>();
        this.failureHooks = new CopyOnWriteArrayList<>();
        this.cancellationHooks = new CopyOnWriteArrayList<>();
        this.trackers = new CopyOnWriteArrayList<>();
        this.timer = new TimerTracker();
        attach(timer);
    }

    public static <T> DUUIFlow<T> pending() {
        return new DUUIFlow<>();
    }

    public static <T> DUUIFlow<T> fromCompletionStages(CompletionStage<?> dispatchStage, CompletionStage<T> completionStage) {
        Objects.requireNonNull(completionStage, "completionStage");
        DUUIFlow<T> flow = pending();
        if (dispatchStage != null) {
            dispatchStage.whenComplete((ignored, throwable) -> {
                if (throwable == null) {
                    flow.dispatchFlow();
                } else {
                    flow.dispatchFlow();
                    flow.failFlow(unwrap(throwable));
                }
            });
        }
        completionStage.whenComplete((value, throwable) -> {
            if (flow.isDone()) {
                return;
            }
            if (!flow.isDispatched()) {
                flow.dispatchFlow();
            }
            if (throwable == null) {
                flow.complete(value);
                return;
            }
            Throwable cause = unwrap(throwable);
            if (cause instanceof CancellationException) {
                flow.cancelFlow(cause);
            } else {
                flow.failFlow(cause);
            }
        });
        return flow;
    }

    public static <T> DUUIFlow<T> dispatch(T value) {
        DUUIFlow<T> flow = pending();
        flow.dispatchFlow();
        flow.complete(value);
        return flow;
    }

    public static DUUIFlow<Void> dispatch() {
        return dispatch(null);
    }

    public static <T> DUUIFlow<T> fail(Throwable throwable) {
        DUUIFlow<T> flow = pending();
        flow.dispatchFlow();
        flow.failFlow(throwable);
        return flow;
    }

    public static <T> DUUIFlow<T> cancel(InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return cancel((Throwable) interrupted);
    }

    public static <T> DUUIFlow<T> cancel(Throwable cause) {
        DUUIFlow<T> flow = pending();
        flow.dispatchFlow();
        flow.cancelFlow(cause);
        return flow;
    }

    public static <T> DUUIFlow<T> cancel() {
        return cancel((Throwable) null);
    }

    public DUUIFlow<T> onDispatch(Runnable hook) {
        Objects.requireNonNull(hook, "hook");
        dispatchHooks.add(hook);
        if (dispatched) {
            run(hook);
        }
        return this;
    }

    public DUUIFlow<T> onCompleted(Consumer<? super T> hook) {
        Objects.requireNonNull(hook, "hook");
        completionHooks.add(hook);
        if (completion.isDone() && !completion.isCompletedExceptionally() && !completion.isCancelled()) {
            run(() -> hook.accept(completion.join()));
        }
        return this;
    }

    public DUUIFlow<T> onFailed(Consumer<? super Throwable> hook) {
        Objects.requireNonNull(hook, "hook");
        failureHooks.add(hook);
        if (completion.isCompletedExceptionally() && !completion.isCancelled()) {
            completion.handle((ignored, throwable) -> {
                if (throwable != null) {
                    run(() -> hook.accept(unwrap(throwable)));
                }
                return null;
            });
        }
        return this;
    }

    public DUUIFlow<T> onCancelled(Consumer<? super Throwable> hook) {
        Objects.requireNonNull(hook, "hook");
        cancellationHooks.add(hook);
        if (completion.isCancelled()) {
            run(() -> hook.accept(cancellationCause));
        }
        return this;
    }

    public DUUIFlow<T> attach(DUUITracker tracker) {
        Objects.requireNonNull(tracker, "tracker");
        trackers.add(tracker);
        if (dispatched) {
            run(tracker::start);
        }
        if (done) {
            run(tracker::stop);
        }
        return this;
    }

    public Optional<DUUIPhase> phase() {
        return Optional.ofNullable(phase);
    }

    DUUIFlow<T> phase(DUUIPhase phase) {
        this.phase = phase;
        return this;
    }

    public Optional<Duration> duration() {
        return timer.duration();
    }

    public CompletableFuture<T> toCompletableFuture() {
        return completion;
    }

    public T join() {
        return completion.join();
    }

    public boolean isDispatched() {
        return dispatched;
    }

    public boolean isDone() {
        return completion.isDone();
    }

    public boolean isCancelled() {
        return completion.isCancelled();
    }

    DUUIFlow<T> dispatchFlow() {
        if (!dispatched) {
            dispatched = true;
            trackers.forEach(tracker -> run(tracker::start));
            dispatchHooks.forEach(this::run);
        }
        return this;
    }

    DUUIFlow<T> complete(T value) {
        if (completion.complete(value)) {
            done = true;
            completionHooks.forEach(hook -> run(() -> hook.accept(value)));
            trackers.forEach(tracker -> run(tracker::stop));
        }
        return this;
    }

    DUUIFlow<T> failFlow(Throwable throwable) {
        Throwable failure = Objects.requireNonNull(throwable, "throwable");
        if (completion.completeExceptionally(failure)) {
            done = true;
            failureHooks.forEach(hook -> run(() -> hook.accept(failure)));
            trackers.forEach(tracker -> run(tracker::stop));
        }
        return this;
    }

    DUUIFlow<T> cancelFlow(Throwable cause) {
        cancellationCause = cause;
        if (completion.cancel(false)) {
            done = true;
            cancellationHooks.forEach(hook -> run(() -> hook.accept(cause)));
            trackers.forEach(tracker -> run(tracker::stop));
        }
        return this;
    }

    private void run(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable ignored) {
            // Flow lifecycle hooks must not hide the original phase result.
        }
    }

    private static Throwable unwrap(Throwable throwable) {
        if (throwable instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        if (throwable instanceof CancellationException cancellationException && cancellationException.getCause() != null) {
            return cancellationException.getCause();
        }
        return throwable;
    }

    private static final class TimerTracker implements DUUITracker {
        private volatile Instant startedAt;
        private volatile Instant stoppedAt;

        @Override
        public void start() {
            startedAt = Instant.now();
        }

        @Override
        public void stop() {
            stoppedAt = Instant.now();
        }

        Optional<Duration> duration() {
            Instant start = startedAt;
            Instant stop = stoppedAt;
            if (start == null || stop == null) {
                return Optional.empty();
            }
            return Optional.of(Duration.between(start, stop));
        }
    }
}

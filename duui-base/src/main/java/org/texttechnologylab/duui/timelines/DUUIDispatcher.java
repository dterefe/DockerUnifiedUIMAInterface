package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.storage.DUUIInMemoryIndex;
import org.texttechnologylab.duui.storage.DUUIInMemoryRegistry;
import org.texttechnologylab.duui.storage.DUUIIndex;
import org.texttechnologylab.duui.storage.DUUIRegistry;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public final class DUUIDispatcher {
    private static final Executor VIRTUAL_EXECUTOR = command -> Thread.ofVirtual()
            .name("duui-phase-virtual-", 0)
            .start(command);
    private static final Executor PLATFORM_EXECUTOR = ForkJoinPool.commonPool();
    private static final ThreadLocal<DUUIDispatchMode> DISPATCH_OVERRIDE = new ThreadLocal<>();

    private final DUUITimeline runtimeTimeline;
    private final DUUIRegistry<String, DUUITimeline> timelines;
    private final DUUIRegistry<String, DUUITracker> trackers;
    private final DUUIIndex<DUUIStatus, String> trackerIndex;

    public DUUIDispatcher() {
        this(new DUUITimeline(null), new DUUIInMemoryRegistry<>(), new DUUIInMemoryRegistry<>(), new DUUIInMemoryIndex<>());
    }

    public DUUIDispatcher(
            DUUITimeline runtimeTimeline,
            DUUIRegistry<String, DUUITimeline> timelines,
            DUUIRegistry<String, DUUITracker> trackers,
            DUUIIndex<DUUIStatus, String> trackerIndex
    ) {
        this.runtimeTimeline = Objects.requireNonNull(runtimeTimeline, "runtimeTimeline");
        this.timelines = Objects.requireNonNull(timelines, "timelines");
        this.trackers = Objects.requireNonNull(trackers, "trackers");
        this.trackerIndex = Objects.requireNonNull(trackerIndex, "trackerIndex");
    }

    public DUUITimeline timeline() {
        return runtimeTimeline;
    }

    public DUUITimeline timeline(DUUIActor actor) {
        Objects.requireNonNull(actor, "actor");
        return timelines.get(actor.id()).orElseGet(() -> {
            DUUITimeline timeline = new DUUITimeline(actor);
            timelines.put(actor.id(), timeline);
            return timeline;
        });
    }

    public DUUIRegistry.Entry<String, DUUITracker> attach(String name, DUUIStatus status, DUUITracker tracker) {
        DUUIRegistry.Entry<String, DUUITracker> entry = trackers.put(name, tracker);
        trackerIndex.add(status, name);
        return entry;
    }

    public <O, T> T dispatch(Invocation<O, T> invocation) throws Exception {
        DUUIDispatchMode mode = dispatchMode(invocation.phase());
        if (mode == null || mode == DUUIDispatchMode.MIXED) {
            return dispatchInline(invocation);
        }
        try {
            return dispatchAsync(invocation).join();
        } catch (CompletionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw error;
        }
    }

    private <O, T> T dispatchInline(Invocation<O, T> invocation) throws Exception {
        DUUIPhase phase = initialize(invocation);
        start(phase);
        try {
            T result = invocation.callable().call();
            finish(phase);
            return result;
        } catch (Throwable throwable) {
            fail(phase, throwable);
            if (throwable instanceof Exception exception) {
                throw exception;
            }
            throw new IllegalStateException(throwable);
        }
    }

    public <O, T> CompletableFuture<T> dispatchAsync(Invocation<O, T> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        DUUIEventService service = DUUIEventService.current();
        DUUIEventContext context = service.currentContext();
        return CompletableFuture.supplyAsync(() -> {
            try {
                return DUUIEventService.callWithCurrent(service, context, () -> dispatchInline(invocation));
            } catch (Exception error) {
                throw new CompletionException(error);
            }
        }, executorFor(dispatchMode(invocation.phase())));
    }

    public <O, T> CompletableFuture<T> dispatchCompletion(Invocation<O, ? extends CompletionStage<T>> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        DUUIEventService service = DUUIEventService.current();
        DUUIEventContext context = service.currentContext();
        return CompletableFuture.supplyAsync(() -> {
            DUUIPhase phase = initialize(invocation);
            start(phase);
            try {
                CompletionStage<T> stage = invocation.callable().call();
                return stage.whenComplete((ignored, throwable) -> {
                    DUUIEventService.runWithCurrent(service, context, () -> {
                        if (throwable == null) {
                            finish(phase);
                        } else {
                            fail(phase, throwable);
                        }
                    });
                });
            } catch (Throwable throwable) {
                fail(phase, throwable);
                throw new CompletionException(throwable);
            }
        }, executorFor(dispatchMode(invocation.phase()))).thenCompose(stage -> stage);
    }

    public static DispatchOverride bindDispatchOverride(DUUIDispatchMode mode) {
        DUUIDispatchMode previous = DISPATCH_OVERRIDE.get();
        if (mode == DUUIDispatchMode.CPU || mode == DUUIDispatchMode.IO) {
            DISPATCH_OVERRIDE.set(mode);
        } else {
            DISPATCH_OVERRIDE.remove();
        }
        return () -> {
            if (previous == null) {
                DISPATCH_OVERRIDE.remove();
            } else {
                DISPATCH_OVERRIDE.set(previous);
            }
        };
    }

    private static DUUIDispatchMode dispatchMode(Phase phase) {
        DUUIDispatchMode override = DISPATCH_OVERRIDE.get();
        return override == null ? phase.dispatch() : override;
    }

    private static Executor executorFor(DUUIDispatchMode mode) {
        if (mode == DUUIDispatchMode.IO) {
            return VIRTUAL_EXECUTOR;
        }
        if (mode == DUUIDispatchMode.CPU) {
            return PLATFORM_EXECUTOR;
        }
        return Runnable::run;
    }

    public interface DispatchOverride extends AutoCloseable {
        @Override
        void close();
    }

    public <O, T> DUUIPhase initialize(Invocation<O, T> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        List<DUUIActor> actors = invocation.actors();
        DUUITimeline timeline = actors.isEmpty() ? runtimeTimeline : timeline(actors.get(0));
        return timeline.create(status(invocation), invocation.phase().dispatch(), invocation.method(), actors);
    }

    public void start(DUUIPhase phase) {
        DUUITimeline timeline = timeline(phase);
        timeline.start(phase);
        DUUIEventContext.setCurrentPhaseId(phase.id());
        DUUIEventContext.setCurrentPhaseStatus(phase.status().name());
        DUUIEventContext.setCurrentPhaseLifecycle(phase.lifecycle().name());
        trackers(phase).forEach(tracker -> tracker.start(phase));
    }

    public void finish(DUUIPhase phase) {
        DUUITimeline timeline = timeline(phase);
        timeline.finish(phase);
        sync(timeline);
        trackers(phase).forEach(tracker -> tracker.finish(phase));
    }

    public void fail(DUUIPhase phase, Throwable throwable) {
        DUUITimeline timeline = timeline(phase);
        timeline.fail(phase, throwable);
        sync(timeline);
        trackers(phase).forEach(tracker -> tracker.fail(phase, throwable));
    }

    private static <O, T> DUUIStatus status(Invocation<O, T> invocation) {
        return invocation.phase().value();
    }

    private DUUITimeline timeline(DUUIPhase phase) {
        return phaseActorsTimeline(phase).orElse(runtimeTimeline);
    }

    private java.util.Optional<DUUITimeline> phaseActorsTimeline(DUUIPhase phase) {
        return timelines.values().stream()
                .filter(timeline -> timeline.phase(phase.id()).isPresent())
                .findFirst();
    }

    private java.util.stream.Stream<DUUITracker> trackers(DUUIPhase phase) {
        return trackerIndex.find(phase.status()).stream().map(trackers::require);
    }

    private void sync(DUUITimeline timeline) {
        timeline.current().ifPresentOrElse(
                p -> {
                    DUUIEventContext.setCurrentPhaseId(p.id());
                    DUUIEventContext.setCurrentPhaseStatus(p.status().name());
                    DUUIEventContext.setCurrentPhaseLifecycle(p.lifecycle().name());
                },
                DUUIEventContext::clearPhase
        );
    }

    public record Invocation<O, T>(
            Phase phase,
            Method method,
            O owner,
            List<DUUIActor> actors,
            Callable<T> callable
    ) {
        public Invocation {
            phase = Objects.requireNonNull(phase, "phase");
            method = Objects.requireNonNull(method, "method");
            actors = List.copyOf(actors == null ? List.of() : actors);
            callable = Objects.requireNonNull(callable, "callable");
        }
    }
}

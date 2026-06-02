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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Callable;

public final class DUUIDispatcher {
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
        try {
            return dispatchFlow(invocation).join();
        } catch (CompletionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw error;
        } catch (CancellationException error) {
            throw error;
        }
    }

    public <O, T> DUUIFlow<T> dispatchFlow(Invocation<O, T> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        DUUIFlow<T> flow = DUUIFlow.pending();
        DUUIPhase phase = initialize(invocation);
        attach(phase, flow);
        DUUIEventService service = DUUIEventService.current();
        DUUIEventContext context = service.currentContext();
        try {
            flow.dispatchFlow();
            T result = DUUIEventService.callWithCurrent(service, context, invocation.callable());
            flow.complete(result);
        } catch (CancellationException error) {
            flow.cancelFlow(error);
        } catch (Exception error) {
            flow.failFlow(error);
        } catch (Throwable throwable) {
            flow.failFlow(throwable);
        }
        return flow;
    }

    public <O, T> DUUIFlow<T> dispatchFlowResult(Invocation<O, DUUIFlow<T>> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        DUUIPhase phase = initialize(invocation);
        DUUIEventService service = DUUIEventService.current();
        DUUIEventContext context = service.currentContext();
        try {
            DUUIFlow<T> returned = DUUIEventService.callWithCurrent(service, context, invocation.callable());
            attach(phase, returned);
            return returned;
        } catch (CancellationException error) {
            DUUIFlow<T> failed = DUUIFlow.pending();
            attach(phase, failed);
            failed.dispatchFlow();
            failed.cancelFlow(error);
            return failed;
        } catch (Exception error) {
            DUUIFlow<T> failed = DUUIFlow.pending();
            attach(phase, failed);
            failed.dispatchFlow();
            failed.failFlow(error);
            return failed;
        } catch (Throwable throwable) {
            DUUIFlow<T> failed = DUUIFlow.pending();
            attach(phase, failed);
            failed.dispatchFlow();
            failed.failFlow(throwable);
            return failed;
        }
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

    public interface DispatchOverride extends AutoCloseable {
        @Override
        void close();
    }

    public <O, T> DUUIPhase initialize(Invocation<O, T> invocation) {
        Objects.requireNonNull(invocation, "invocation");
        List<DUUIActor> actors = invocation.actors();
        DUUITimeline timeline = actors.isEmpty() ? runtimeTimeline : timeline(actors.get(0));
        return timeline.create(
                status(invocation),
                invocation.phase().dispatch(),
                invocation.method(),
                invocation.owner(),
                invocation.arguments(),
                actors
        );
    }

    private <T> void attach(DUUIPhase phase, DUUIFlow<T> flow) {
        phase.attach(flow);
        flow.phase(phase)
                .onDispatch(() -> start(phase))
                .onCompleted(ignored -> finish(phase))
                .onFailed(throwable -> fail(phase, throwable))
                .onCancelled(throwable -> cancel(phase));
        trackerIndex.find(phase.status()).stream().map(trackers::require).forEach(flow::attach);
    }

    public void start(DUUIPhase phase) {
        DUUITimeline timeline = timeline(phase);
        timeline.start(phase);
        DUUIEventContext.setCurrentPhase(phase);
    }

    public void finish(DUUIPhase phase) {
        DUUITimeline timeline = timeline(phase);
        timeline.finish(phase);
        sync(timeline);
    }

    public void fail(DUUIPhase phase, Throwable throwable) {
        DUUITimeline timeline = timeline(phase);
        timeline.fail(phase, throwable);
        sync(timeline);
    }

    public void cancel(DUUIPhase phase) {
        DUUITimeline timeline = timeline(phase);
        timeline.cancel(phase);
        sync(timeline);
    }

    private static <O, T> DUUIStatus status(Invocation<O, T> invocation) {
        return invocation.phase().value();
    }

    private DUUITimeline timeline(DUUIPhase phase) {
        return timelines.values().stream()
                .filter(timeline -> timeline.phase(phase.id()).isPresent())
                .findFirst()
                .orElse(runtimeTimeline);
    }

    private void sync(DUUITimeline timeline) {
        timeline.current().ifPresentOrElse(
                p -> {
                    DUUIEventContext.setCurrentPhase(p);
                },
                () -> {
                    DUUIEventContext.clearPhase();
                }
        );
    }

    public record Invocation<O, T>(
            Phase phase,
            Method method,
            O owner,
            List<DUUIActor> actors,
            List<Object> arguments,
            Callable<T> callable
    ) {
        public Invocation(
                Phase phase,
                Method method,
                O owner,
                List<DUUIActor> actors,
                Callable<T> callable
        ) {
            this(phase, method, owner, actors, List.of(), callable);
        }

        public Invocation {
            phase = Objects.requireNonNull(phase, "phase");
            method = Objects.requireNonNull(method, "method");
            actors = List.copyOf(actors == null ? List.of() : actors);
            arguments = arguments == null || arguments.isEmpty()
                    ? List.of()
                    : Collections.unmodifiableList(new ArrayList<>(arguments));
            callable = Objects.requireNonNull(callable, "callable");
        }
    }
}

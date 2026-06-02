package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.event.DUUIEventContext;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

public final class DUUIComposerDispatcher {
    private final DUUITimeline timeline;

    public DUUIComposerDispatcher() {
        this(new DUUITimeline(null));
    }

    public DUUIComposerDispatcher(DUUITimeline timeline) {
        this.timeline = Objects.requireNonNull(timeline, "timeline");
    }

    public DUUITimeline timeline() {
        return timeline;
    }

    public <O, T> T dispatch(Phase phase, Method method, O owner, List<DUUIActor> actors, List<Object> arguments, Callable<T> callable) throws Exception {
        try {
            return dispatchFlow(phase, method, owner, actors, arguments, callable).join();
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

    public <O, T> DUUIFlow<T> dispatchFlow(Phase phase, Method method, O owner, List<DUUIActor> actors, List<Object> arguments, Callable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        DUUIFlow<T> flow = DUUIFlow.pending();
        DUUIPhase phaseInstance = initialize(phase, method, owner, actors, arguments);
        phaseInstance.attach(flow);
        flow.phase(phaseInstance)
                .onDispatch(() -> {
                    timeline.start(phaseInstance);
                    DUUIEventContext.setCurrentPhase(phaseInstance);
                })
                .onCompleted(ignored -> {
                    timeline.finish(phaseInstance);
                    timeline.current().ifPresentOrElse(
                            DUUIEventContext::setCurrentPhase,
                            DUUIEventContext::clearPhase
                    );
                })
                .onFailed(throwable -> {
                    timeline.fail(phaseInstance, throwable);
                    timeline.current().ifPresentOrElse(
                            DUUIEventContext::setCurrentPhase,
                            DUUIEventContext::clearPhase
                    );
                })
                .onCancelled(throwable -> {
                    timeline.cancel(phaseInstance);
                    timeline.current().ifPresentOrElse(
                            DUUIEventContext::setCurrentPhase,
                            DUUIEventContext::clearPhase
                    );
                });
        try {
            flow.dispatchFlow();
            flow.complete(callable.call());
        } catch (CancellationException error) {
            flow.cancelFlow(error);
        } catch (Exception error) {
            flow.failFlow(error);
        } catch (Throwable throwable) {
            flow.failFlow(throwable);
        }
        return flow;
    }

    public <O> DUUIPhase initialize(Phase phase, Method method, O owner, List<DUUIActor> actors, List<Object> arguments) {
        return timeline.create(
                Objects.requireNonNull(phase, "phase").value(),
                phase.dispatch(),
                Objects.requireNonNull(method, "method"),
                owner,
                List.copyOf(arguments == null ? List.of() : arguments),
                actors
        );
    }

}

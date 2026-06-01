package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.ems.DUUIActor;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Callable;

public final class DUUIPhaseAspect {
    private final DUUIDispatcher dispatcher;

    public DUUIPhaseAspect(DUUIDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public <O, T> T around(O owner, Method method, List<DUUIActor> actors, Callable<T> callable) throws Exception {
        return dispatcher.dispatch(new DUUIDispatcher.Invocation<>(
                method.getAnnotation(Phase.class),
                method,
                owner,
                actors,
                callable
        ));
    }

    public <O, T> CompletableFuture<T> aroundAsync(O owner, Method method, List<DUUIActor> actors, Callable<T> callable) {
        return dispatcher.dispatchAsync(new DUUIDispatcher.Invocation<>(
                method.getAnnotation(Phase.class),
                method,
                owner,
                actors,
                callable
        ));
    }

    public <O, T> CompletableFuture<T> aroundCompletion(O owner, Method method, List<DUUIActor> actors, Callable<? extends CompletionStage<T>> callable) {
        return dispatcher.dispatchCompletion(new DUUIDispatcher.Invocation<>(
                method.getAnnotation(Phase.class),
                method,
                owner,
                actors,
                callable
        ));
    }
}

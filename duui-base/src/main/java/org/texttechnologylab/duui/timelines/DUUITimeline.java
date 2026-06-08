package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.DUUIWorkerContext;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.storage.DUUIInMemoryIndex;
import org.texttechnologylab.duui.storage.DUUIInMemoryRegistry;
import org.texttechnologylab.duui.storage.DUUIIndex;
import org.texttechnologylab.duui.storage.DUUIRegistry;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;

public final class DUUITimeline {
    private static final String KEY_PHASE_STACK = "timeline.phase.stack";

    private final DUUIActor actor;
    private final DUUIRegistry<String, DUUIPhase> phases;
    private final DUUIIndex<String, String> children;
    private final DUUIIndex<String, String> actors;
    private final DUUIIndex<String, String> phaseActors;

    public DUUITimeline(DUUIActor actor) {
        this(actor, new DUUIInMemoryRegistry<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>(), new DUUIInMemoryIndex<>());
    }

    public DUUITimeline(
            DUUIActor actor,
            DUUIRegistry<String, DUUIPhase> phases,
            DUUIIndex<String, String> children,
            DUUIIndex<String, String> actors,
            DUUIIndex<String, String> phaseActors
    ) {
        this.actor = actor;
        this.phases = Objects.requireNonNull(phases, "phases");
        this.children = Objects.requireNonNull(children, "children");
        this.actors = Objects.requireNonNull(actors, "actors");
        this.phaseActors = Objects.requireNonNull(phaseActors, "phaseActors");
    }

    public Optional<DUUIActor> actor() {
        return Optional.ofNullable(actor);
    }

    public Optional<DUUIPhase> current() {
        Deque<String> stack = phaseStack();
        String id = stack.peek();
        return id == null ? Optional.empty() : phases.get(id);
    }

    public DUUIPhase create(DUUIStatus status, Method method, List<DUUIActor> actors) {
        return create(status, DUUIDispatchMode.MIXED, method, actors);
    }

    public DUUIPhase create(DUUIStatus status, DUUIDispatchMode dispatchMode, Method method, List<DUUIActor> actors) {
        return create(status, dispatchMode, method, null, List.of(), actors);
    }

    public DUUIPhase create(
            DUUIStatus status,
            DUUIDispatchMode dispatchMode,
            Method method,
            Object owner,
            List<?> parameters,
            List<DUUIActor> actors
    ) {
        Optional<DUUIPhase> parent = current();
        List<DUUIActor> phaseActors = List.copyOf(actors == null ? List.of() : actors);
        DUUIPhase phase = new DUUIPhase(
                status,
                dispatchMode,
                method,
                parent.map(DUUIPhase::id).orElse(null),
                owner,
                parameters,
                phaseActors
        );
        phases.put(phase.id(), phase);
        parent.ifPresent(value -> children.add(value.id(), phase.id()));
        phaseActors.forEach(value -> {
            this.actors.add(value.id(), phase.id());
            this.phaseActors.add(phase.id(), value.id());
        });
        return phase;
    }

    public DUUIPhase start(DUUIPhase phase) {
        phase.start();
        phaseStack().push(phase.id());
        return phase;
    }

    public DUUIPhase finish(DUUIPhase phase) {
        phase.finish();
        leave(phase);
        return phase;
    }

    public DUUIPhase fail(DUUIPhase phase, Throwable throwable) {
        phase.fail(throwable);
        leave(phase);
        return phase;
    }

    public DUUIPhase cancel(DUUIPhase phase) {
        phase.cancel();
        leave(phase);
        return phase;
    }

    public Stream<DUUIPhase> phases() {
        return phases.values().stream();
    }

    public Optional<DUUIPhase> phase(String id) {
        return phases.get(id);
    }

    public Stream<DUUIPhase> children(DUUIPhase phase) {
        Objects.requireNonNull(phase, "phase");
        return children.find(phase.id()).stream().map(phases::require);
    }

    public Stream<DUUIPhase> phases(DUUIActor actor) {
        Objects.requireNonNull(actor, "actor");
        return actors.find(actor.id()).stream().map(phases::require);
    }

    public Stream<String> actors(DUUIPhase phase) {
        Objects.requireNonNull(phase, "phase");
        return phaseActors.find(phase.id()).stream();
    }

    public Stream<DUUIPhase> lineage(DUUIPhase phase) {
        Objects.requireNonNull(phase, "phase");
        Deque<DUUIPhase> lineage = new ArrayDeque<>();
        Optional<DUUIPhase> current = Optional.of(phase);
        while (current.isPresent()) {
            DUUIPhase value = current.get();
            lineage.push(value);
            current = value.parent().flatMap(phases::get);
        }
        return lineage.stream();
    }

    @SuppressWarnings("unchecked")
    private Deque<String> phaseStack() {
        DUUIWorkerContext ctx = DUUIWorkerContext.current();
        Deque<String> stack = ctx.get(KEY_PHASE_STACK);
        if (stack == null) {
            stack = new ArrayDeque<>();
            ctx.set(KEY_PHASE_STACK, stack);
        }
        return stack;
    }

    private void leave(DUUIPhase phase) {
        Deque<String> stack = phaseStack();
        if (Objects.equals(stack.peek(), phase.id())) {
            stack.pop();
            return;
        }
        stack.remove(phase.id());
    }
}

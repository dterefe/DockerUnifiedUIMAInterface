package org.texttechnologylab.duui.timelines;

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
    private final DUUIActor actor;
    private final DUUIRegistry<String, DUUIPhase> phases;
    private final DUUIIndex<String, String> children;
    private final DUUIIndex<String, String> actors;
    private final DUUIIndex<String, String> phaseActors;
    private final ThreadLocal<Deque<String>> context = ThreadLocal.withInitial(ArrayDeque::new);

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
        String id = context.get().peek();
        return id == null ? Optional.empty() : phases.get(id);
    }

    public DUUIPhase create(DUUIStatus status, Method method, List<DUUIActor> actors) {
        return create(status, DUUIDispatchMode.MIXED, method, actors);
    }

    public DUUIPhase create(DUUIStatus status, DUUIDispatchMode dispatchMode, Method method, List<DUUIActor> actors) {
        Optional<DUUIPhase> parent = current();
        DUUIPhase phase = new DUUIPhase(status, dispatchMode, method, parent.map(DUUIPhase::id).orElse(null));
        phases.put(phase.id(), phase);
        parent.ifPresent(value -> children.add(value.id(), phase.id()));
        actors.forEach(value -> {
            this.actors.add(value.id(), phase.id());
            this.phaseActors.add(phase.id(), value.id());
        });
        return phase;
    }

    public DUUIPhase start(DUUIPhase phase) {
        phase.start();
        context.get().push(phase.id());
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

    private void leave(DUUIPhase phase) {
        Deque<String> stack = context.get();
        if (Objects.equals(stack.peek(), phase.id())) {
            stack.pop();
            return;
        }
        stack.remove(phase.id());
    }
}

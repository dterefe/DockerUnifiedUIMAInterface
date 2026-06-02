package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.ems.DUUIResource;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class DUUIStage<T> implements DUUIResource {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final String name;
    private final DUUIStageType type;
    private final DUUIExecutionMode executionMode;
    private final List<DUUIComponent<T>> components;
    private final Object operation;
    private final DUUICheckpoint<?> output;
    private final DUUICheckpoint<T> continuation;
    private final String componentId;
    private final DUUIDispatchPolicy dispatchPolicy;
    private final DUUIFailurePolicy failurePolicy;

    private DUUIStage(
            String id,
            String name,
            DUUIStageType type,
            DUUIExecutionMode executionMode,
            List<DUUIComponent<T>> components,
            Object operation,
            DUUICheckpoint<?> output,
            DUUICheckpoint<T> continuation,
            DUUIDispatchPolicy dispatchPolicy,
            DUUIFailurePolicy failurePolicy
    ) {
        this.gid = GID.create(DUUIStage.class);
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.name = name == null ? id : name;
        this.type = Objects.requireNonNull(type, "type");
        this.executionMode = executionMode == null ? DUUIExecutionMode.LINEAR : executionMode;
        this.components = Collections.unmodifiableList(new ArrayList<>(components == null ? List.of() : components));
        this.operation = operation;
        this.output = output;
        this.continuation = continuation;
        this.componentId = id;
        this.dispatchPolicy = dispatchPolicy == null ? DUUIDispatchPolicy.INHERIT : dispatchPolicy;
        this.failurePolicy = failurePolicy;
    }

    public static <T> DUUIStage<T> processor(String id, DUUIExecutionMode mode, List<DUUIComponent<T>> components, DUUICheckpoint<T> output, DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A processor stage requires at least one component.");
        }
        return new DUUIStage<>(id, id, DUUIStageType.PROCESSOR, mode, components, null, output, null, dispatchPolicy, failurePolicy);
    }

    public static <A, B> DUUIStage<A> adapter(String id, DUUIAdapter<A, B> adapter, DUUICheckpoint<B> output) {
        return new DUUIStage<>(id, id, DUUIStageType.ADAPTER, DUUIExecutionMode.LINEAR, List.of(), adapter, output, null, DUUIDispatchPolicy.INHERIT, null);
    }

    public static <P, C> DUUIStage<P> fork(String id, DUUIFork<P, C> fork, DUUICheckpoint<C> output, DUUICheckpoint<P> continuation) {
        return new DUUIStage<>(id, id, DUUIStageType.FORK, DUUIExecutionMode.LINEAR, List.of(), fork, output, continuation, DUUIDispatchPolicy.INHERIT, null);
    }

    public static <I, O> DUUIStage<I> split(String id, DUUISplit<I, O> split, DUUICheckpoint<O> output, DUUICheckpoint<I> continuation) {
        return new DUUIStage<>(id, id, DUUIStageType.SPLIT, DUUIExecutionMode.LINEAR, List.of(), split, output, continuation, DUUIDispatchPolicy.INHERIT, null);
    }

    public static <I, O> DUUIStage<I> join(String id, DUUIJoin<I, O> join, DUUICheckpoint<O> output) {
        return new DUUIStage<>(id, id, DUUIStageType.JOIN, DUUIExecutionMode.LINEAR, List.of(), join, output, null, DUUIDispatchPolicy.INHERIT, null);
    }

    public static <T> DUUIStage<T> target(String id, DUUITarget<T> target) {
        return new DUUIStage<>(id, id, DUUIStageType.TARGET, DUUIExecutionMode.LINEAR, List.of(), target, null, null, DUUIDispatchPolicy.INHERIT, null);
    }

    public DUUIStage<T> withPolicies(DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        return new DUUIStage<>(id, name, type, executionMode, components, operation, output, continuation,
                dispatchPolicy == null ? this.dispatchPolicy : dispatchPolicy,
                failurePolicy == null ? this.failurePolicy : failurePolicy);
    }

    @Override
    public GID gid() { return gid; }
    @Override
    public DUUITraits traits() { return traits; }
    @Override
    public String id() { return id; }
    public String name() { return name; }
    public DUUIStageType type() { return type; }
    public DUUIExecutionMode executionMode() { return executionMode; }
    public List<DUUIComponent<T>> components() { return components; }
    public Object operation() { return operation; }
    public DUUICheckpoint<?> output() { return output; }
    public DUUICheckpoint<T> continuation() { return continuation; }
    public String componentId() { return componentId; }
    public DUUIDispatchPolicy dispatchPolicy() { return dispatchPolicy; }
    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }
}

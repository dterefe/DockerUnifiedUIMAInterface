package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.DUUIDispatchPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class DUUIStage<T> {
    private final String id;
    private final String name;
    private final DUUIStageType type;
    private final List<DUUIComponent<T>> components;
    private final String componentId;
    private final DUUIDispatchPolicy dispatchPolicy;
    private final DUUIFailurePolicy failurePolicy;

    public DUUIStage(String id, String name, DUUIComponent<T> component) {
        this(id, name, DUUIStageType.LINEAR, List.of(component), id, DUUIDispatchPolicy.INHERIT, null);
    }

    public DUUIStage(String id, String name, DUUIComponent<T> component, String componentId, DUUIFailurePolicy failurePolicy) {
        this(id, name, DUUIStageType.LINEAR, List.of(component), componentId, DUUIDispatchPolicy.INHERIT, failurePolicy);
    }

    public DUUIStage(
            String id,
            String name,
            DUUIStageType type,
            List<DUUIComponent<T>> components,
            String componentId,
            DUUIDispatchPolicy dispatchPolicy,
            DUUIFailurePolicy failurePolicy
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = name == null ? id : name;
        this.type = type == null ? DUUIStageType.LINEAR : type;
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A DUUIStage requires at least one component.");
        }
        this.components = Collections.unmodifiableList(new ArrayList<>(components));
        this.componentId = componentId == null ? id : componentId;
        this.dispatchPolicy = dispatchPolicy == null ? DUUIDispatchPolicy.INHERIT : dispatchPolicy;
        this.failurePolicy = failurePolicy;
    }

    public static <T> DUUIStage<T> of(String id, DUUIComponent<T> component) {
        return new DUUIStage<>(id, id, component);
    }

    public static <T> DUUIStage<T> linear(String id, List<DUUIComponent<T>> components) {
        return new DUUIStage<>(id, id, DUUIStageType.LINEAR, components, id, DUUIDispatchPolicy.INHERIT, null);
    }

    public static <T> DUUIStage<T> parallel(String id, List<DUUIComponent<T>> components) {
        return new DUUIStage<>(id, id, DUUIStageType.PARALLEL, components, id, DUUIDispatchPolicy.mixed(), null);
    }

    public String id() { return id; }
    public String name() { return name; }
    public DUUIStageType type() { return type; }
    public List<DUUIComponent<T>> components() { return components; }
    public DUUIComponent<T> component() { return components.get(0); }
    public boolean forks() { return components.stream().anyMatch(DUUIForkComponent.class::isInstance); }
    public String componentId() { return componentId; }
    public DUUIDispatchPolicy dispatchPolicy() { return dispatchPolicy; }
    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }
}

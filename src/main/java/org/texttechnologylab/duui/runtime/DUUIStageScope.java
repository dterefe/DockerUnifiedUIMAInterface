package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.DUUIDispatchPolicy;
import org.texttechnologylab.duui.pipeline.DUUIComponent;
import org.texttechnologylab.duui.pipeline.DUUILambda;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.apache.uima.jcas.JCas;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DUUIStageScope<T> implements AutoCloseable {
    private final DUUICheckpointScope<T> checkpoint;
    private final DUUIFlowScope<T> flow;
    private final String id;
    private final boolean parallel;
    private final List<DUUIComponent<T>> components = new ArrayList<>();
    private final List<DUUIStageContribution> contributions = new ArrayList<>();
    private DUUIDispatchPolicy dispatchPolicy;
    private DUUIFailurePolicy failurePolicy;
    private boolean closed;

    DUUIStageScope(DUUICheckpointScope<T> checkpoint, String id, boolean parallel) {
        this.checkpoint = checkpoint;
        this.flow = null;
        this.id = Objects.requireNonNull(id, "id");
        this.parallel = parallel;
    }

    DUUIStageScope(DUUIFlowScope<T> flow, String id, boolean parallel) {
        this.checkpoint = null;
        this.flow = flow;
        this.id = Objects.requireNonNull(id, "id");
        this.parallel = parallel;
    }

    public DUUIStageScope<T> component(DUUIComponent<T> component) {
        components.add(Objects.requireNonNull(component, "component"));
        return this;
    }

    public DUUIStageScope<T> lambda(DUUILambda<T> lambda) {
        Objects.requireNonNull(lambda, "lambda");
        if (flow != null && !flow.artifactType().equals(lambda.inputType())) {
            throw new IllegalArgumentException("Lambda input type does not match stage flow.");
        }
        components.add(DUUIComponent.processor(lambda.inputType().id() + "-lambda", lambda::process));
        return this;
    }

    public DUUIV1ComponentBuilder v1(String id) {
        DUUIV1ComponentBuilder builder = new DUUIV1ComponentBuilder(this, id);
        contributions.add(builder);
        return builder;
    }

    public DUUIUimaComponentBuilder uima(String id) {
        DUUIUimaComponentBuilder builder = new DUUIUimaComponentBuilder(this, id);
        contributions.add(builder);
        return builder;
    }

    @SuppressWarnings("unchecked")
    void jcasComponent(DUUIComponent<JCas> component) {
        components.add((DUUIComponent<T>) component);
    }

    public DUUIStageScope<T> dispatchPolicy(DUUIDispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = dispatchPolicy;
        return this;
    }

    public DUUIStageScope<T> failurePolicy(DUUIFailurePolicy failurePolicy) {
        this.failurePolicy = failurePolicy;
        return this;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (DUUIStageContribution contribution : contributions) {
            contribution.contribute();
        }
        DUUIStage<T> stage = parallel
                ? DUUIStage.parallel(id, components)
                : DUUIStage.linear(id, components);
        if (dispatchPolicy != null || failurePolicy != null) {
            stage = new DUUIStage<>(
                    stage.id(),
                    stage.name(),
                    stage.type(),
                    stage.components(),
                    stage.componentId(),
                    dispatchPolicy == null ? stage.dispatchPolicy() : dispatchPolicy,
                    failurePolicy == null ? stage.failurePolicy() : failurePolicy
            );
        }
        if (flow == null) {
            checkpoint.addStage(stage);
        } else {
            flow.addStage(stage);
        }
    }
}

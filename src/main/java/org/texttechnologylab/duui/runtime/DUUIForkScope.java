package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.component.DUUIComponents;
import org.texttechnologylab.duui.pipeline.DUUIFork;
import org.texttechnologylab.duui.pipeline.DUUIStage;

import java.util.List;

public final class DUUIForkScope<P, C> extends DUUIAbstractFlowScope<C> {
    private final DUUIFlowScope<P> parent;
    private final DUUIFork<P, C> fork;

    DUUIForkScope(DUUIFlowScope<P> parent, DUUIFork<P, C> fork) {
        super(parent.pipeline(), fork.outputType());
        this.parent = parent;
        this.fork = fork;
        if (!parent.artifactType().equals(fork.inputType())) {
            throw new IllegalArgumentException("Fork input type does not match parent flow.");
        }
        parent.addStage(DUUIStage.linear(fork.outputType().id() + "-fork", List.of(DUUIComponents.fork(fork))));
    }

    public DUUIFork<P, C> fork() {
        return fork;
    }

    public DUUIFlowScope<P> parent() {
        return parent;
    }
}

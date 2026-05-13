package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUIComponents;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUITarget;

import java.util.List;

public final class DUUITargetScope<T> implements AutoCloseable {
    private boolean closed;

    DUUITargetScope(DUUIFlowScope<T> parent, DUUITarget<T> target) {
        if (!parent.artifactType().equals(target.inputType())) {
            throw new IllegalArgumentException("Target input type does not match parent flow.");
        }
        parent.addStage(DUUIStage.linear(target.inputType().id() + "-target", List.of(DUUIComponents.target(target))));
    }

    @Override
    public void close() {
        closed = true;
    }
}

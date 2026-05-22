package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUITarget;

public final class DUUITargetScope<T> implements AutoCloseable {
    private boolean closed;

    DUUITargetScope(DUUIFlowScope<T> parent, DUUITarget<T> target) {
        parent.addStage(DUUIStage.target(parent.checkpoint().id() + "-target", target));
    }

    @Override
    public void close() {
        closed = true;
    }
}

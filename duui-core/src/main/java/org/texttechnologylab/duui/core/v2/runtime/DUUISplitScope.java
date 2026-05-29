package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUISplit;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public final class DUUISplitScope<I, O> extends DUUIAbstractFlowScope<O> {
    private final DUUIFlowScope<I> parent;
    private final DUUISplit<I, O> split;

    DUUISplitScope(DUUIFlowScope<I> parent, DUUISplit<I, O> split) {
        super(parent.pipeline(), parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-split-out"));
        this.parent = parent;
        this.split = split;
        @SuppressWarnings("unchecked")
        DUUICheckpoint<O> output = (DUUICheckpoint<O>) checkpoint();
        DUUICheckpoint<I> continuation = parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-split-join");
        parent.addStage(DUUIStage.split(parent.checkpoint().id() + "-split", split, output, continuation));
    }

    public DUUISplit<I, O> split() {
        return split;
    }

    public DUUIFlowScope<I> parent() {
        return parent;
    }
}

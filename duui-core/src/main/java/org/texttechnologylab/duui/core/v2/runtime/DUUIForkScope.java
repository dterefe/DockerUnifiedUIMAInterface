package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIFork;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public final class DUUIForkScope<P, C> extends DUUIAbstractFlowScope<C> {
    private final DUUIFlowScope<P> parent;
    private final DUUIFork<P, C> fork;

    DUUIForkScope(DUUIFlowScope<P> parent, DUUIFork<P, C> fork) {
        super(parent.pipeline(), parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-fork-out"));
        this.parent = parent;
        this.fork = fork;
        @SuppressWarnings("unchecked")
        DUUICheckpoint<C> output = (DUUICheckpoint<C>) checkpoint();
        DUUICheckpoint<P> continuation = parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-fork-join");
        parent.addStage(DUUIStage.fork(parent.checkpoint().id() + "-fork", fork, output, continuation));
    }

    public DUUIFork<P, C> fork() {
        return fork;
    }

    public DUUIFlowScope<P> parent() {
        return parent;
    }
}

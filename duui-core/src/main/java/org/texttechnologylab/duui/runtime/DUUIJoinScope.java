package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIJoin;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public final class DUUIJoinScope<I, O> extends DUUIAbstractFlowScope<O> {
    private final DUUIFlowScope<I> parent;
    private final DUUIJoin<I, O> join;

    DUUIJoinScope(DUUIFlowScope<I> parent, DUUIJoin<I, O> join) {
        super(parent.pipeline(), parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-join-out"));
        this.parent = parent;
        this.join = join;
        @SuppressWarnings("unchecked")
        DUUICheckpoint<O> output = (DUUICheckpoint<O>) checkpoint();
        parent.addStage(DUUIStage.join(parent.checkpoint().id() + "-join", join, output));
    }

    public DUUIJoin<I, O> join() {
        return join;
    }

    public DUUIFlowScope<I> parent() {
        return parent;
    }
}

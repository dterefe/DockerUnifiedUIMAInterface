package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public final class DUUIAdapterScope<A, B> extends DUUIAbstractFlowScope<B> {
    private final DUUIFlowScope<A> parent;
    private final DUUIAdapter<A, B> adapter;

    DUUIAdapterScope(DUUIFlowScope<A> parent, DUUIAdapter<A, B> adapter) {
        super(parent.pipeline(), parent.pipeline().createCheckpoint(parent.checkpoint().id() + "-adapter-out"));
        this.parent = parent;
        this.adapter = adapter;
        @SuppressWarnings("unchecked")
        DUUICheckpoint<B> output = (DUUICheckpoint<B>) checkpoint();
        parent.addStage(DUUIStage.adapter(parent.checkpoint().id() + "-adapter", adapter, output));
    }

    public DUUIAdapter<A, B> adapter() {
        return adapter;
    }

    public DUUIFlowScope<A> parent() {
        return parent;
    }
}

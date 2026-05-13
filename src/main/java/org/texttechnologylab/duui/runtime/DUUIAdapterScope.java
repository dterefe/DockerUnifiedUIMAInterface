package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUIAdapter;
import org.texttechnologylab.duui.pipeline.DUUIComponents;
import org.texttechnologylab.duui.pipeline.DUUIStage;

import java.util.List;

public final class DUUIAdapterScope<A, B> extends DUUIAbstractFlowScope<B> {
    private final DUUIFlowScope<A> parent;
    private final DUUIAdapter<A, B> adapter;

    DUUIAdapterScope(DUUIFlowScope<A> parent, DUUIAdapter<A, B> adapter) {
        super(parent.pipeline(), adapter.outputType());
        this.parent = parent;
        this.adapter = adapter;
        if (!parent.artifactType().equals(adapter.inputType())) {
            throw new IllegalArgumentException("Adapter input type does not match parent flow.");
        }
        parent.addStage(DUUIStage.linear(adapter.outputType().id() + "-adapter", List.of(DUUIComponents.adapter(adapter))));
    }

    public DUUIAdapter<A, B> adapter() {
        return adapter;
    }

    public DUUIFlowScope<A> parent() {
        return parent;
    }
}

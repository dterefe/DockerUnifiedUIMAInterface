package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUISource;

public final class DUUIGeneratorScope<T> extends DUUIAbstractFlowScope<T> {
    private final DUUISource<T> source;

    DUUIGeneratorScope(DUUIPipelineScope pipeline, DUUISource<T> source, DUUICheckpoint<T> output) {
        super(pipeline, output);
        this.source = source;
        pipeline.registerSource(source, output);
    }

    public DUUISource<T> generator() {
        return source;
    }

    public DUUISource<T> source() {
        return source;
    }
}

package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUIGenerator;

public final class DUUIGeneratorScope<T> extends DUUIAbstractFlowScope<T> {
    private final DUUIGenerator<T> generator;

    DUUIGeneratorScope(DUUIPipelineScope pipeline, DUUIGenerator<T> generator) {
        super(pipeline, generator.outputType());
        this.generator = generator;
        pipeline.registerGenerator(generator);
    }

    public DUUIGenerator<T> generator() {
        return generator;
    }
}

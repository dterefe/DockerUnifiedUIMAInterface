package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifactEmitter;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.runtime.DUUIGeneratorScope;
import org.texttechnologylab.duui.runtime.DUUIPipelineScope;

public interface DUUIGenerator<T> {
    DUUIArtifactType<T> outputType();

    void generate(DUUIArtifactEmitter<T> emitter) throws Exception;

    default DUUIGeneratorScope<T> open(DUUIPipelineScope pipeline) {
        return pipeline.add(this);
    }
}

package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public interface DUUIFlowScope<T> extends AutoCloseable {
    DUUIArtifactType<T> artifactType();

    DUUIStageScope<T> linear(String id);

    DUUIStageScope<T> parallel(String id);

    void addStage(DUUIStage<T> stage);

    DUUIPipelineScope pipeline();

    @Override
    void close();
}

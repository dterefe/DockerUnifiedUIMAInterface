package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIStage;

public interface DUUIFlowScope<T> extends AutoCloseable {
    DUUICheckpoint<T> checkpoint();

    DUUIStageScope<T> linear(String id);

    DUUIStageScope<T> parallel(String id);

    DUUICheckpoint<T> addStage(DUUIStage<T> stage);

    DUUIPipelineScope pipeline();

    @Override
    void close();
}

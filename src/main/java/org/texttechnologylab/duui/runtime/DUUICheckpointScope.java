package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUICheckpointConfig;
import org.texttechnologylab.duui.pipeline.DUUIStage;

import java.util.Objects;

public final class DUUICheckpointScope<T> implements AutoCloseable {
    private final DUUIPipelineScope pipeline;
    private final DUUICheckpoint<T> checkpoint;
    private DUUICheckpointConfig config;
    private DUUIFailurePolicy failurePolicy;
    private boolean closed;

    DUUICheckpointScope(DUUIPipelineScope pipeline, DUUICheckpoint<T> checkpoint) {
        this.pipeline = pipeline;
        this.checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
    }

    public DUUIStageScope<T> linear(String id) {
        return new DUUIStageScope<>(this, id, false);
    }

    public DUUIStageScope<T> parallel(String id) {
        return new DUUIStageScope<>(this, id, true);
    }

    public DUUICheckpointScope<T> stage(DUUIStage<T> stage) {
        checkpoint.stage(stage);
        return this;
    }

    public DUUICheckpointScope<T> config(DUUICheckpointConfig config) {
        this.config = config;
        return this;
    }

    public DUUICheckpointScope<T> failurePolicy(DUUIFailurePolicy failurePolicy) {
        this.failurePolicy = failurePolicy;
        return this;
    }

    DUUICheckpoint<T> checkpoint() {
        return checkpoint;
    }

    DUUIPipelineScope pipeline() {
        return pipeline;
    }

    DUUICheckpoint<T> addStage(DUUIStage<T> stage) {
        checkpoint.stage(stage);
        return stage.output() == null ? checkpoint : cast(stage.output());
    }

    @SuppressWarnings("unchecked")
    private DUUICheckpoint<T> cast(DUUICheckpoint<?> checkpoint) {
        return (DUUICheckpoint<T>) checkpoint;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        pipeline.checkpoint(checkpoint);
    }
}

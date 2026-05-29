package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIStage;

abstract class DUUIAbstractFlowScope<T> implements DUUIFlowScope<T> {
    private final DUUIPipelineScope pipeline;
    private DUUICheckpoint<T> checkpoint;
    private boolean closed;

    DUUIAbstractFlowScope(DUUIPipelineScope pipeline, DUUICheckpoint<T> checkpoint) {
        this.pipeline = pipeline;
        this.checkpoint = checkpoint;
        this.pipeline.checkpoint(checkpoint);
    }

    @Override
    public DUUICheckpoint<T> checkpoint() {
        return checkpoint;
    }

    @Override
    public DUUIStageScope<T> linear(String id) {
        return new DUUIStageScope<>(this, id, false);
    }

    @Override
    public DUUIStageScope<T> parallel(String id) {
        return new DUUIStageScope<>(this, id, true);
    }

    @Override
    public DUUICheckpoint<T> addStage(DUUIStage<T> stage) {
        checkpoint.stage(stage);
        if (stage.continuation() != null) {
            this.checkpoint = stage.continuation();
        } else if (stage.output() != null && stage.output() instanceof DUUICheckpoint<?> next) {
            @SuppressWarnings("unchecked")
            DUUICheckpoint<T> typed = (DUUICheckpoint<T>) next;
            this.checkpoint = typed;
        }
        return this.checkpoint;
    }

    void current(DUUICheckpoint<T> checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Override
    public DUUIPipelineScope pipeline() {
        return pipeline;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
        }
    }
}

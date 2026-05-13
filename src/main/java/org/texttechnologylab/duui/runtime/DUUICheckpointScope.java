package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUICheckpointConfig;
import org.texttechnologylab.duui.pipeline.DUUIStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DUUICheckpointScope<T> implements AutoCloseable {
    private final DUUIPipelineScope pipeline;
    private final String id;
    private final DUUIArtifactType<T> artifactType;
    private final List<DUUIStage<T>> stages = new ArrayList<>();
    private DUUICheckpointConfig config;
    private DUUIFailurePolicy failurePolicy;
    private boolean closed;

    DUUICheckpointScope(DUUIPipelineScope pipeline, String id, DUUIArtifactType<T> artifactType) {
        this.pipeline = pipeline;
        this.id = Objects.requireNonNull(id, "id");
        this.artifactType = Objects.requireNonNull(artifactType, "artifactType");
    }

    public DUUIStageScope<T> linear(String id) {
        return new DUUIStageScope<>(this, id, false);
    }

    public DUUIStageScope<T> parallel(String id) {
        return new DUUIStageScope<>(this, id, true);
    }

    public DUUICheckpointScope<T> stage(DUUIStage<T> stage) {
        stages.add(stage);
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

    void addStage(DUUIStage<T> stage) {
        stages.add(stage);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        DUUICheckpoint.Builder<T> checkpoint = DUUICheckpoint.route(id, artifactType)
                .config(config)
                .failurePolicy(failurePolicy);
        for (DUUIStage<T> stage : stages) {
            checkpoint.stage(stage);
        }
        pipeline.checkpoint(checkpoint.build());
    }
}

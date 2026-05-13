package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.artifact.DUUIArtifactType;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUIStage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DUUIPipelineScope implements AutoCloseable {
    private final DUUISystemScope system;
    private final String id;
    private final List<DUUIGenerator<?>> generators = new ArrayList<>();
    private final Map<DUUIArtifactType<?>, DUUICheckpoint.Builder<?>> flowCheckpoints = new LinkedHashMap<>();
    private final List<DUUICheckpoint<?>> checkpoints = new ArrayList<>();
    private DUUIFailurePolicy failurePolicy;
    private boolean closed;

    DUUIPipelineScope(DUUISystemScope system, String id) {
        this.system = system;
        this.id = Objects.requireNonNull(id, "id");
    }

    public <T> DUUICheckpointScope<T> checkpoint(String id, DUUIArtifactType<T> artifactType) {
        return new DUUICheckpointScope<>(this, id, artifactType);
    }

    public <T> DUUIGeneratorScope<T> add(DUUIGenerator<T> generator) {
        return new DUUIGeneratorScope<>(this, generator);
    }

    public <A, B> DUUIAdapterScope<A, B> adapter(DUUIFlowScope<A> parent, org.texttechnologylab.duui.pipeline.DUUIAdapter<A, B> adapter) {
        return new DUUIAdapterScope<>(parent, adapter);
    }

    public <P, C> DUUIForkScope<P, C> fork(DUUIFlowScope<P> parent, org.texttechnologylab.duui.pipeline.DUUIFork<P, C> fork) {
        return new DUUIForkScope<>(parent, fork);
    }

    public <T> DUUITargetScope<T> target(DUUIFlowScope<T> parent, org.texttechnologylab.duui.pipeline.DUUITarget<T> target) {
        return new DUUITargetScope<>(parent, target);
    }

    public <T> DUUICheckpointScope<T> checkpoint(String id, Class<T> payloadType) {
        return checkpoint(id, DUUIArtifactType.javaType(payloadType));
    }

    public DUUIPipelineScope failurePolicy(DUUIFailurePolicy failurePolicy) {
        this.failurePolicy = failurePolicy;
        return this;
    }

    void checkpoint(DUUICheckpoint<?> checkpoint) {
        checkpoints.add(checkpoint);
    }

    <T> void registerGenerator(DUUIGenerator<T> generator) {
        generators.add(generator);
        ensureCheckpoint(generator.outputType());
    }

    <T> void ensureCheckpoint(DUUIArtifactType<T> artifactType) {
        checkpointBuilder(artifactType);
    }

    <T> void addStage(DUUIArtifactType<T> artifactType, DUUIStage<T> stage) {
        checkpointBuilder(artifactType).stage(stage);
    }

    @SuppressWarnings("unchecked")
    private <T> DUUICheckpoint.Builder<T> checkpointBuilder(DUUIArtifactType<T> artifactType) {
        return (DUUICheckpoint.Builder<T>) flowCheckpoints.computeIfAbsent(
                artifactType,
                type -> DUUICheckpoint.route(type.id(), artifactType)
        );
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        DUUIPipeline.Builder pipeline = DUUIPipeline.builder(id).failurePolicy(failurePolicy);
        for (DUUIGenerator<?> generator : generators) {
            pipeline.generator(generator);
        }
        for (DUUICheckpoint.Builder<?> checkpoint : flowCheckpoints.values()) {
            pipeline.checkpoint(checkpoint.build());
        }
        for (DUUICheckpoint<?> checkpoint : checkpoints) {
            pipeline.checkpoint(checkpoint);
        }
        system.pipeline(pipeline.build());
    }
}

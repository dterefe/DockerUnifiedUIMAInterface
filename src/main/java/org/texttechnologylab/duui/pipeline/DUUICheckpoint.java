package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.artifact.DUUIArtifactType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class DUUICheckpoint<T> {
    private final String id;
    private final Class<T> payloadType;
    private final DUUIArtifactType<T> artifactType;
    private final List<DUUIStage<T>> stages;
    private final DUUICheckpointConfig config;
    private final DUUIFailurePolicy failurePolicy;

    private DUUICheckpoint(Builder<T> builder) {
        this.id = Objects.requireNonNull(builder.id, "id");
        this.payloadType = builder.payloadType;
        this.artifactType = Objects.requireNonNull(builder.artifactType, "artifactType");
        this.stages = Collections.unmodifiableList(new ArrayList<>(builder.stages));
        this.config = builder.config == null ? DUUICheckpointConfig.DEFAULT : builder.config;
        this.failurePolicy = builder.failurePolicy;
    }

    public static <T> Builder<T> builder(String id, Class<T> payloadType) {
        return new Builder<>(id, payloadType, DUUIArtifactType.javaType(payloadType));
    }

    public static <T> Builder<T> route(String id, DUUIArtifactType<T> artifactType) {
        return new Builder<>(id, null, artifactType);
    }

    public String id() { return id; }
    public Class<T> payloadType() { return payloadType; }
    public DUUIArtifactType<T> artifactType() { return artifactType; }
    public List<DUUIStage<T>> stages() { return stages; }
    public DUUICheckpointConfig config() { return config; }
    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }

    public static final class Builder<T> {
        private final String id;
        private final Class<T> payloadType;
        private final DUUIArtifactType<T> artifactType;
        private final List<DUUIStage<T>> stages = new ArrayList<>();
        private DUUICheckpointConfig config;
        private DUUIFailurePolicy failurePolicy;

        private Builder(String id, Class<T> payloadType, DUUIArtifactType<T> artifactType) {
            this.id = id;
            this.payloadType = payloadType;
            this.artifactType = artifactType;
        }

        public Builder<T> stage(DUUIStage<T> stage) { stages.add(stage); return this; }
        public Builder<T> component(String id, DUUIComponent<T> component) { stages.add(DUUIStage.of(id, component)); return this; }
        public Builder<T> config(DUUICheckpointConfig config) { this.config = config; return this; }
        public Builder<T> failurePolicy(DUUIFailurePolicy failurePolicy) { this.failurePolicy = failurePolicy; return this; }
        public DUUICheckpoint<T> build() { return new DUUICheckpoint<>(this); }
    }
}

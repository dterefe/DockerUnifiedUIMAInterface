package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIResource;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class DUUICheckpoint<T> implements DUUIResource {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final Queue<DUUIArtifact<T>> queue = new ConcurrentLinkedQueue<>();
    private final DUUICheckpointConfig config;
    private final DUUIFailurePolicy failurePolicy;
    private DUUIStage<T> stage;

    public DUUICheckpoint(String id) {
        this(id, DUUICheckpointConfig.DEFAULT, null);
    }

    public DUUICheckpoint(String id, DUUICheckpointConfig config, DUUIFailurePolicy failurePolicy) {
        this.gid = GID.create();
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.config = config == null ? DUUICheckpointConfig.DEFAULT : config;
        this.failurePolicy = failurePolicy;
    }

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    @Override
    public String id() {
        return id;
    }

    public Queue<DUUIArtifact<T>> queue() {
        return queue;
    }

    public DUUIStage<T> stage() {
        return stage;
    }

    public void stage(DUUIStage<T> stage) {
        if (this.stage != null) {
            throw new IllegalStateException("DUUICheckpoint already owns a stage: " + id);
        }
        this.stage = Objects.requireNonNull(stage, "stage");
    }

    public DUUICheckpointConfig config() {
        return config;
    }

    public DUUIFailurePolicy failurePolicy() {
        return failurePolicy;
    }
}

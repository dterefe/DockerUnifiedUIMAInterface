package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIResource;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Checkpoint queues idle {@link DUUIArtifact} objects before dispatch to a stage.
 * Migrated from raw {@code ConcurrentLinkedQueue} to {@link DUUIPool} for telemetry.
 *
 * [DESIGN: lines 95-101, 286]
 *
 * @param <T> artifact payload type
 */
public final class DUUICheckpoint<T> implements DUUIResource {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final DUUIPool<DUUIArtifact<T>> pool;
    private final DUUICheckpointConfig config;
    private final DUUIFailurePolicy failurePolicy;
    private DUUIStage<T> stage;

    public DUUICheckpoint(String id) {
        this(id, DUUICheckpointConfig.DEFAULT, null);
    }

    public DUUICheckpoint(String id, DUUICheckpointConfig config, DUUIFailurePolicy failurePolicy) {
        this.gid = GID.create(DUUICheckpoint.class);
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.config = config == null ? DUUICheckpointConfig.DEFAULT : config;
        this.failurePolicy = failurePolicy;
        this.pool = new DUUIPool<>(gid, id, new LinkedBlockingQueue<>());
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

    /**
     * The DUUIPool holding queued artifacts awaiting dispatch.
     *
     * @return pool of artifacts
     */
    public DUUIPool<DUUIArtifact<T>> pool() {
        return pool;
    }

    /**
     * Convenience: offer an artifact to the pool.
     */
    public boolean offer(DUUIArtifact<T> artifact) {
        return pool.offer(artifact);
    }

    /**
     * Convenience: take an artifact from the pool (blocking).
     */
    public DUUIArtifact<T> take() throws InterruptedException {
        return pool.take();
    }

    /**
     * Convenience: poll an artifact from the pool (non-blocking).
     */
    public DUUIArtifact<T> poll() {
        return pool.poll();
    }

    /**
     * Convenience: current number of queued artifacts.
     */
    public int depth() {
        return pool.depth();
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

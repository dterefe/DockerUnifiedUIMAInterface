package org.texttechnologylab.duui.artifact;

import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.ems.DUUISubject;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.DUUITimeline;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Artifact wrapper that implements {@link AutoCloseable} for try-with-resources access.
 * Maintains internal timeline tracking for phase transitions and metadata propagation.
 *
 * <p>Lifecycle events tracked per artifact:</p>
 * <ul>
 *   <li>{@code creation} — timestamp when the artifact was created</li>
 *   <li>{@code stage_entry} — when artifact entered a processing stage</li>
 *   <li>{@code stage_exit} — when artifact exited a processing stage</li>
 *   <li>{@code processing_start} — when processing on this artifact began</li>
 *   <li>{@code processing_end} — when processing on this artifact completed</li>
 * </ul>
 *
 * [DESIGN: lines 118-126, 233-260]
 */
public final class DUUIArtifact<T> implements DUUISubject, DUUIActor, AutoCloseable {
    private final GID gid;
    private final T subject;
    private final DUUITraits traits;
    private final Map<String, Object> metadata;
    private final DUUITimeline timeline;

    // Lifecycle timestamps
    private final Instant createdAt;
    private volatile Instant stageEntryAt;
    private volatile Instant stageExitAt;
    private volatile Instant processingStartAt;
    private volatile Instant processingEndAt;

    private DUUIArtifact(GID gid, T subject, DUUITraits traits, Map<String, Object> metadata) {
        this.gid = Objects.requireNonNull(gid, "gid");
        this.subject = Objects.requireNonNull(subject, "subject");
        this.traits = traits == null ? DUUITraits.empty() : traits;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
        this.timeline = new DUUITimeline(this);
        this.createdAt = Instant.now();
    }

    public static <T> DUUIArtifact<T> of(T subject) {
        return new DUUIArtifact<>(GID.create(DUUIArtifact.class), subject, DUUITraits.empty(), null);
    }

    public static <T> DUUIArtifact<T> of(T subject, DUUITraits traits) {
        return new DUUIArtifact<>(GID.create(DUUIArtifact.class), subject, traits, null);
    }

    /**
     * Create an artifact with metadata (e.g., from DUUIFile deserialization).
     *
     * @param subject  the wrapped payload
     * @param traits   artifact traits
     * @param metadata structured metadata map
     * @param <T>      payload type
     * @return new artifact with metadata
     */
    public static <T> DUUIArtifact<T> of(T subject, DUUITraits traits, Map<String, Object> metadata) {
        return new DUUIArtifact<>(GID.create(DUUIArtifact.class), subject, traits, metadata);
    }

    public DUUIArtifact<T> withTraits(DUUITraits traits) {
        DUUIArtifact<T> copy = new DUUIArtifact<>(gid, subject, traits, metadata);
        copy.stageEntryAt = this.stageEntryAt;
        copy.stageExitAt = this.stageExitAt;
        copy.processingStartAt = this.processingStartAt;
        copy.processingEndAt = this.processingEndAt;
        return copy;
    }

    /**
     * Create a copy with additional metadata merged in.
     *
     * @param additional metadata to merge
     * @return new artifact with merged metadata
     */
    public DUUIArtifact<T> withMetadata(Map<String, Object> additional) {
        if (additional == null || additional.isEmpty()) {
            return this;
        }
        Map<String, Object> merged = new LinkedHashMap<>(metadata.isEmpty() ? new LinkedHashMap<>() : metadata);
        merged.putAll(additional);
        DUUIArtifact<T> copy = new DUUIArtifact<>(gid, subject, traits, merged);
        copy.stageEntryAt = this.stageEntryAt;
        copy.stageExitAt = this.stageExitAt;
        copy.processingStartAt = this.processingStartAt;
        copy.processingEndAt = this.processingEndAt;
        return copy;
    }

    // ─── Timeline lifecycle tracking ───────────────────────────────

    /**
     * Mark this artifact as having entered a stage.
     */
    public void markStageEntry() {
        this.stageEntryAt = Instant.now();
    }

    /**
     * Mark this artifact as having exited a stage.
     */
    public void markStageExit() {
        this.stageExitAt = Instant.now();
    }

    /**
     * Mark the start of processing on this artifact.
     */
    public void markProcessingStart() {
        this.processingStartAt = Instant.now();
    }

    /**
     * Mark the end of processing on this artifact.
     */
    public void markProcessingEnd() {
        this.processingEndAt = Instant.now();
    }

    // ─── Accessors ─────────────────────────────────────────────────

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    public T subject() {
        return subject;
    }

    public T payload() {
        return subject;
    }

    /**
     * Structured metadata carried through artifact timeline during transformations.
     *
     * @return unmodifiable metadata map
     */
    public Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * The artifact's timeline for phase tracking.
     *
     * @return the DUUITimeline instance
     */
    public DUUITimeline timeline() {
        return timeline;
    }

    /**
     * Creation timestamp.
     */
    public Instant createdAt() {
        return createdAt;
    }

    /**
     * When this artifact last entered a stage, or null.
     */
    public Instant stageEntryAt() {
        return stageEntryAt;
    }

    /**
     * When this artifact last exited a stage, or null.
     */
    public Instant stageExitAt() {
        return stageExitAt;
    }

    /**
     * When processing started on this artifact, or null.
     */
    public Instant processingStartAt() {
        return processingStartAt;
    }

    /**
     * When processing ended on this artifact, or null.
     */
    public Instant processingEndAt() {
        return processingEndAt;
    }

    /**
     * Close the artifact's current timeline.
     * Records final timeline events. Called at end of try-with-resources blocks.
     *
     * [DESIGN: lines 118-126]
     */
    @Override
    public void close() {
        if (processingStartAt != null && processingEndAt == null) {
            this.processingEndAt = Instant.now();
        }
        if (stageEntryAt != null && stageExitAt == null) {
            this.stageExitAt = Instant.now();
        }
    }
}

package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pipeline: an ordered sequence of {@link DUUIStage} components.
 * Owns the {@link DUUIScheduler} for checkpoint selection.
 *
 * <p>Stages are executed in order. Source must be first, Target must be last.
 * Source and Target cardinality = 1 per pipeline, enforced at build time.</p>
 *
 * [DESIGN: lines 71-92, 95-101, 103-104]
 */
public final class DUUIPipeline {
    private final String id;
    private final List<DUUIStage<?>> stages;
    private final DUUIScheduler scheduler;
    private final DUUIFailurePolicy failurePolicy;

    private DUUIPipeline(Builder builder) {
        this.id = builder.id;
        this.stages = Collections.unmodifiableList(new ArrayList<>(builder.stages));
        this.scheduler = builder.scheduler != null ? builder.scheduler : new DUUIScheduler();
        this.failurePolicy = builder.failurePolicy;
        validate();
    }

    /**
     * Create a new builder for the given pipeline ID.
     */
    public static Builder builder(String id) {
        return new Builder(id);
    }

    private void validate() {
        if (stages.isEmpty()) {
            throw new IllegalStateException("Pipeline requires at least one stage.");
        }
        boolean hasSource = stages.stream().anyMatch(s -> s.type() == DUUIStageType.SOURCE);
        boolean hasTarget = stages.stream().anyMatch(s -> s.type() == DUUIStageType.TARGET);

        if (hasSource || hasTarget) {
            // Source must be first
            if (stages.get(0).type() != DUUIStageType.SOURCE) {
                throw new IllegalStateException("Pipeline must start with a SOURCE stage. Found: " + stages.get(0).type());
            }
            // Target must be last
            if (stages.get(stages.size() - 1).type() != DUUIStageType.TARGET) {
                throw new IllegalStateException("Pipeline must end with a TARGET stage. Found: " + stages.get(stages.size() - 1).type());
            }
            // Source single-use enforcement
            long sourceCount = stages.stream().filter(s -> s.type() == DUUIStageType.SOURCE).count();
            if (sourceCount != 1) {
                throw new IllegalStateException("Pipeline must have exactly one SOURCE stage. Found: " + sourceCount);
            }
            // Target single-use enforcement
            long targetCount = stages.stream().filter(s -> s.type() == DUUIStageType.TARGET).count();
            if (targetCount != 1) {
                throw new IllegalStateException("Pipeline must have exactly one TARGET stage. Found: " + targetCount);
            }
            // Ensure Target is terminal (no stages follow it)
            for (int i = 0; i < stages.size() - 1; i++) {
                if (stages.get(i).type() == DUUIStageType.TARGET) {
                    throw new IllegalStateException("TARGET stage must be the last stage. Found another stage after it.");
                }
            }
        }
    }

    public String id() { return id; }

    /**
     * Ordered list of stages in this pipeline.
     */
    public List<DUUIStage<?>> stages() { return stages; }

    /**
     * The scheduler owned by this pipeline for checkpoint selection.
     */
    public DUUIScheduler scheduler() { return scheduler; }

    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }

    /**
     * Convenience: get all checkpoints from stages that have output.
     * Computed from stage input/output DUUIPools.
     */
    public List<DUUICheckpoint<?>> checkpoints() {
        List<DUUICheckpoint<?>> result = new ArrayList<>();
        for (DUUIStage<?> stage : stages) {
            if (stage.output() != null) {
                result.add(stage.output());
            }
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Get the source (first) stage.
     */
    public DUUIStage<?> source() {
        return stages.get(0);
    }

    /**
     * Get the target (last) stage.
     */
    public DUUIStage<?> target() {
        return stages.get(stages.size() - 1);
    }

    public static final class Builder {
        private final String id;
        private final List<DUUIStage<?>> stages = new ArrayList<>();
        private DUUIScheduler scheduler;
        private DUUIFailurePolicy failurePolicy;

        private Builder(String id) {
            this.id = id;
        }

        /**
         * Add a stage to the pipeline. Stages execute in the order they are added.
         */
        public Builder stage(DUUIStage<?> stage) {
            stages.add(stage);
            return this;
        }

        /**
         * Add a stage via its checkpoint. If the checkpoint has no stage assigned,
         * it is registered as a passive checkpoint (e.g., terminal done checkpoint).
         */
        public Builder checkpoint(DUUICheckpoint<?> checkpoint) {
            if (checkpoint.stage() != null) {
                stages.add(checkpoint.stage());
            }
            return this;
        }

        /**
         * Set the scheduler for this pipeline.
         */
        public Builder scheduler(DUUIScheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        /**
         * Set the failure policy.
         */
        public Builder failurePolicy(DUUIFailurePolicy failurePolicy) {
            this.failurePolicy = failurePolicy;
            return this;
        }

        /**
         * Build the pipeline. Validates stage ordering, cardinality, and terminal position.
         */
        public DUUIPipeline build() {
            return new DUUIPipeline(this);
        }
    }
}

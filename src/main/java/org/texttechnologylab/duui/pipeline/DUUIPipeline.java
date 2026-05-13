package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DUUIPipeline {
    private final String id;
    private final List<DUUIGenerator<?>> generators;
    private final List<DUUICheckpoint<?>> checkpoints;
    private final DUUIFailurePolicy failurePolicy;

    private DUUIPipeline(Builder builder) {
        this.id = builder.id;
        this.generators = Collections.unmodifiableList(new ArrayList<>(builder.generators));
        this.checkpoints = Collections.unmodifiableList(new ArrayList<>(builder.checkpoints));
        this.failurePolicy = builder.failurePolicy;
    }

    public static Builder builder(String id) { return new Builder(id); }

    public String id() { return id; }
    public List<DUUIGenerator<?>> generators() { return generators; }
    public List<DUUICheckpoint<?>> checkpoints() { return checkpoints; }
    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }

    public static final class Builder {
        private final String id;
        private final List<DUUIGenerator<?>> generators = new ArrayList<>();
        private final List<DUUICheckpoint<?>> checkpoints = new ArrayList<>();
        private DUUIFailurePolicy failurePolicy;

        private Builder(String id) { this.id = id; }
        public Builder generator(DUUIGenerator<?> generator) { generators.add(generator); return this; }
        public Builder checkpoint(DUUICheckpoint<?> checkpoint) { checkpoints.add(checkpoint); return this; }
        public Builder failurePolicy(DUUIFailurePolicy failurePolicy) { this.failurePolicy = failurePolicy; return this; }
        public DUUIPipeline build() { return new DUUIPipeline(this); }
    }
}

package org.texttechnologylab.duui.runtime;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUISource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DUUIPipelineScope implements AutoCloseable {
    private final DUUISystemScope system;
    private final String id;
    private final List<DUUIPipeline.SourceBinding<?>> sources = new ArrayList<>();
    private final List<DUUICheckpoint<?>> checkpoints = new ArrayList<>();
    private DUUIFailurePolicy failurePolicy;
    private boolean closed;
    private int checkpointCounter;

    DUUIPipelineScope(DUUISystemScope system, String id) {
        this.system = system;
        this.id = Objects.requireNonNull(id, "id");
    }

    public <T> DUUICheckpointScope<T> checkpoint(String id) {
        return new DUUICheckpointScope<>(this, new DUUICheckpoint<>(id));
    }

    public <T> DUUIGeneratorScope<T> add(DUUISource<T> source) {
        DUUICheckpoint<T> output = createCheckpoint("source-" + (++checkpointCounter));
        return new DUUIGeneratorScope<>(this, source, output);
    }

    public <A, B> DUUIAdapterScope<A, B> adapter(DUUIFlowScope<A> parent, org.texttechnologylab.duui.pipeline.DUUIAdapter<A, B> adapter) {
        return new DUUIAdapterScope<>(parent, adapter);
    }

    public <P, C> DUUIForkScope<P, C> fork(DUUIFlowScope<P> parent, org.texttechnologylab.duui.pipeline.DUUIFork<P, C> fork) {
        return new DUUIForkScope<>(parent, fork);
    }

    public <I, O> DUUISplitScope<I, O> split(DUUIFlowScope<I> parent, org.texttechnologylab.duui.pipeline.DUUISplit<I, O> split) {
        return new DUUISplitScope<>(parent, split);
    }

    public <I, O> DUUIJoinScope<I, O> join(DUUIFlowScope<I> parent, org.texttechnologylab.duui.pipeline.DUUIJoin<I, O> join) {
        return new DUUIJoinScope<>(parent, join);
    }

    public <T> DUUITargetScope<T> target(DUUIFlowScope<T> parent, org.texttechnologylab.duui.pipeline.DUUITarget<T> target) {
        return new DUUITargetScope<>(parent, target);
    }

    public DUUIPipelineScope failurePolicy(DUUIFailurePolicy failurePolicy) {
        this.failurePolicy = failurePolicy;
        return this;
    }

    <T> DUUICheckpoint<T> createCheckpoint(String id) {
        DUUICheckpoint<T> checkpoint = new DUUICheckpoint<>(id);
        checkpoint(checkpoint);
        return checkpoint;
    }

    void checkpoint(DUUICheckpoint<?> checkpoint) {
        if (!checkpoints.contains(checkpoint)) {
            checkpoints.add(checkpoint);
        }
    }

    <T> void registerSource(DUUISource<T> source, DUUICheckpoint<T> output) {
        sources.add(new DUUIPipeline.SourceBinding<>(source, output));
        checkpoint(output);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        DUUIPipeline.Builder pipeline = DUUIPipeline.builder(id).failurePolicy(failurePolicy);
        for (DUUIPipeline.SourceBinding<?> source : sources) {
            sourceUnchecked(pipeline, source);
        }
        for (DUUICheckpoint<?> checkpoint : checkpoints) {
            pipeline.checkpoint(checkpoint);
        }
        system.pipeline(pipeline.build());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void sourceUnchecked(DUUIPipeline.Builder pipeline, DUUIPipeline.SourceBinding source) {
        pipeline.source(source.source(), source.output());
    }
}

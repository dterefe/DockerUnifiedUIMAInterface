package org.texttechnologylab.duui.orchestration.scheduling;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.DUUIStageType;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.DUUITask;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduler owned by {@code DUUIPipeline}.
 * Selects checkpoints from pipeline stages using configurable {@link DUUISchedulerPolicy}.
 *
 * [DESIGN: lines 114-136, 73-74]
 */
public final class DUUIScheduler implements DUUIActor {
    private final DUUISchedulerPolicy policy;

    public DUUIScheduler() {
        this(DUUISchedulerPolicy.firstReady());
    }

    public DUUIScheduler(DUUISchedulerPolicy policy) {
        this.policy = policy == null ? DUUISchedulerPolicy.firstReady() : policy;
    }

    /**
     * Build a snapshot from pipeline stages and their DUUIPools.
     * Only PROCESSOR and ADAPTER stages with non-null output checkpoints are included.
     */
    public static DUUISchedulerPolicy.Snapshot snapshotFrom(List<DUUIStage<?>> stages, int inFlight, DUUIExecutor executor) {
        Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools = new LinkedHashMap<>();
        for (DUUIStage<?> stage : stages) {
            DUUIStageType type = stage.type();
            if (type == DUUIStageType.SOURCE || type == DUUIStageType.TARGET) {
                continue;
            }
            DUUICheckpoint<?> output = stage.output();
            if (output != null) {
                @SuppressWarnings("unchecked")
                DUUIPool<DUUIArtifact<?>> pool = (DUUIPool<DUUIArtifact<?>>) (DUUIPool<?>) output.pool();
                pools.put(output, pool);
            }
        }
        return new DUUISchedulerPolicy.Snapshot(pools, inFlight, executor);
    }

    /**
     * Select the next artifact to process from available checkpoints.
     */
    public Selection select(Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools) {
        return select(pools, 0, null);
    }

    /**
     * Select the next artifact to process, considering in-flight count and executor.
     */
    public Selection select(Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools, int inFlight, DUUIExecutor executor) {
        DUUISchedulerPolicy.Selection selected = policy.select(new DUUISchedulerPolicy.Snapshot(pools, inFlight, executor));
        if (selected == null) {
            return null;
        }
        return new Selection(selected.checkpoint(), selected.artifact());
    }

    /**
     * Select using pipeline stages directly.
     */
    public Selection selectFromStages(List<DUUIStage<?>> stages, int inFlight, DUUIExecutor executor) {
        DUUISchedulerPolicy.Snapshot snapshot = snapshotFrom(stages, inFlight, executor);
        DUUISchedulerPolicy.Selection selected = policy.select(snapshot);
        if (selected == null) {
            return null;
        }
        return new Selection(selected.checkpoint(), selected.artifact());
    }

    public <T> DUUITask<T> dispatch(DUUITask<T> task, DUUIExecutor executor, DUUIDispatchPolicy dispatchPolicy) {
        DUUIDispatchPolicy policy = dispatchPolicy == null ? DUUIDispatchPolicy.mixed() : dispatchPolicy;
        if (policy.caller()) {
            return executor.runInline(task);
        }
        executor.submit(task, policy);
        return task;
    }

    public static boolean canDispatch(int inFlight, DUUIDispatchPolicy dispatchPolicy) {
        if (dispatchPolicy == null || dispatchPolicy.parallelism() == null) {
            return true;
        }
        return inFlight < Math.max(1, dispatchPolicy.parallelism());
    }

    public DUUISchedulerPolicy policy() {
        return policy;
    }

    public record Selection(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
    }
}

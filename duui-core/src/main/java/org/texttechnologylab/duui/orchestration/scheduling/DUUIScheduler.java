package org.texttechnologylab.duui.orchestration.scheduling;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.DUUITask;

import java.util.Map;
import java.util.Queue;

public final class DUUIScheduler implements DUUIActor {
    private final DUUISchedulerPolicy policy;

    public DUUIScheduler() {
        this(DUUISchedulerPolicy.firstReady());
    }

    public DUUIScheduler(DUUISchedulerPolicy policy) {
        this.policy = policy == null ? DUUISchedulerPolicy.firstReady() : policy;
    }

    public Selection select(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues) {
        return select(queues, 0, null);
    }

    public Selection select(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues, int inFlight, DUUIExecutor executor) {
        DUUISchedulerPolicy.Selection selected = policy.select(new DUUISchedulerPolicy.Snapshot(queues, inFlight, executor));
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

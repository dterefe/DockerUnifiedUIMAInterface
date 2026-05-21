package org.texttechnologylab.duui.orchestration.scheduling;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.DUUITask;

import java.util.Map;
import java.util.Queue;

public final class DUUIScheduler {

    public Selection select(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues) {
        return select(queues, 0, null);
    }

    public Selection select(Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues, int inFlight, DUUIExecutor executor) {
        if (queues == null || queues.isEmpty()) {
            return null;
        }
        for (Map.Entry<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> entry : queues.entrySet()) {
            Queue<DUUIArtifact<?>> queue = entry.getValue();
            if (queue == null || queue.isEmpty()) {
                continue;
            }
            DUUIArtifact<?> artifact = queue.peek();
            if (executor != null && !canDispatch(inFlight, executor.dispatchPolicyFor(entry.getKey(), artifact))) {
                continue;
            }
            return new Selection(entry.getKey(), queue.remove());
        }
        return null;
    }

    public <T> DUUITask<T> dispatch(DUUITask<T> task, DUUIExecutor executor, DUUIDispatchPolicy dispatchPolicy) {
        DUUIDispatchPolicy policy = dispatchPolicy == null ? DUUIDispatchPolicy.mixed() : dispatchPolicy;
        if (policy.caller()) {
            return executor.runInline(task);
        }
        executor.submit(task, policy);
        return task;
    }

    public boolean canDispatch(int inFlight, DUUIDispatchPolicy dispatchPolicy) {
        if (dispatchPolicy == null || dispatchPolicy.parallelism() == null) {
            return true;
        }
        return inFlight < Math.max(1, dispatchPolicy.parallelism());
    }

    public record Selection(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
    }
}

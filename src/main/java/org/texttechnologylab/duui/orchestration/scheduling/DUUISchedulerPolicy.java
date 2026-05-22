package org.texttechnologylab.duui.orchestration.scheduling;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;

import java.util.Map;
import java.util.Objects;
import java.util.Queue;

public interface DUUISchedulerPolicy {
    Selection select(Snapshot snapshot);

    static DUUISchedulerPolicy firstReady() {
        return new FirstReady();
    }

    record Snapshot(
            Map<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> queues,
            int inFlight,
            DUUIExecutor executor
    ) {
        public Snapshot {
            queues = queues == null ? Map.of() : queues;
        }
    }

    record Selection(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
        public Selection {
            Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(artifact, "artifact");
        }
    }

    final class FirstReady implements DUUISchedulerPolicy {
        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.queues().isEmpty()) {
                return null;
            }
            for (Map.Entry<DUUICheckpoint<?>, Queue<DUUIArtifact<?>>> entry : snapshot.queues().entrySet()) {
                Queue<DUUIArtifact<?>> queue = entry.getValue();
                if (queue == null || queue.isEmpty()) {
                    continue;
                }
                DUUIArtifact<?> artifact = queue.peek();
                DUUIExecutor executor = snapshot.executor();
                if (executor != null && !DUUIScheduler.canDispatch(snapshot.inFlight(), executor.dispatchPolicyFor(entry.getKey(), artifact))) {
                    continue;
                }
                return new Selection(entry.getKey(), queue.remove());
            }
            return null;
        }
    }
}

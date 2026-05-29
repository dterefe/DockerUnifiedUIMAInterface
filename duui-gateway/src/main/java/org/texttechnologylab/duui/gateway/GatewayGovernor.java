package org.texttechnologylab.duui.gateway;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.governance.DUUIGovernor;
import org.texttechnologylab.duui.gateway.model.GatewayRunSnapshot;
import org.texttechnologylab.duui.gateway.store.GatewayStorage;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.storage.DUUIStoredEvent;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class GatewayGovernor implements DUUIGovernor {
    private final GatewayStorage storage;
    private final Map<String, Counters> counters = new ConcurrentHashMap<>();

    public GatewayGovernor(GatewayStorage storage) {
        this.storage = storage;
    }

    @Override
    public void onRunStarted(String orchestratorId, DUUIPipeline pipeline, Map<String, Object> attributes) {
        counters.put(orchestratorId, new Counters(Instant.now()));
        put(orchestratorId, pipeline, "running", attributes);
        event("INFO", "run.started", orchestratorId, "DUUI run started for pipeline " + pipeline.id(), attributes);
    }

    @Override
    public void onArtifactQueued(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, Map<String, Object> attributes) {
        counters.computeIfAbsent(orchestratorId, ignored -> new Counters(Instant.now())).queued.incrementAndGet();
        put(orchestratorId, pipeline, "running", merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id()));
        event("DEBUG", "artifact.queued", orchestratorId, "Artifact queued at checkpoint " + checkpoint.id(),
                merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id()));
    }

    @Override
    public void onTaskScheduled(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, DUUITask<?> task, Map<String, Object> attributes) {
        counters.computeIfAbsent(orchestratorId, ignored -> new Counters(Instant.now())).scheduled.incrementAndGet();
        put(orchestratorId, pipeline, "running", merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id(), "task", task.id()));
        event("DEBUG", "task.scheduled", orchestratorId, "Task scheduled " + task.id(),
                merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id(), "task", task.id()));
    }

    @Override
    public void onTaskCompleted(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, DUUIExecutionResult<?> result, Map<String, Object> attributes) {
        counters.computeIfAbsent(orchestratorId, ignored -> new Counters(Instant.now())).completed.incrementAndGet();
        Map<String, Object> completed = merge(attributes,
                "artifact", artifact.id(),
                "checkpoint", checkpoint.id(),
                "status", result.status().name(),
                "failure", failureMap(result.failure()));
        put(orchestratorId, pipeline, "running", completed);
        event("INFO", "task.completed", orchestratorId, "Task completed at checkpoint " + checkpoint.id(),
                completed);
    }

    @Override
    public void onTaskFailed(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, Throwable error, Map<String, Object> attributes) {
        counters.computeIfAbsent(orchestratorId, ignored -> new Counters(Instant.now())).failed.incrementAndGet();
        put(orchestratorId, pipeline, "failed", merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id(), "error", error == null ? null : error.getMessage()));
        event("ERROR", "task.failed", orchestratorId, "Task failed at checkpoint " + checkpoint.id(),
                merge(attributes, "artifact", artifact.id(), "checkpoint", checkpoint.id(), "error", error == null ? null : error.getMessage()));
    }

    @Override
    public void onRunCompleted(String orchestratorId, DUUIPipeline pipeline, DUUIOrchestrationResult result, Map<String, Object> attributes) {
        put(orchestratorId, pipeline, result.hasFailures() ? "failed" : "completed", attributes);
        event(result.hasFailures() ? "WARN" : "INFO", "run.completed", orchestratorId,
                "DUUI run completed for pipeline " + pipeline.id(), attributes);
    }

    private void put(String orchestratorId, DUUIPipeline pipeline, String status, Map<String, Object> attributes) {
        Counters current = counters.computeIfAbsent(orchestratorId, ignored -> new Counters(Instant.now()));
        Map<String, Object> previous = storage.runs().get(orchestratorId)
                .map(GatewayRunSnapshot::attributes)
                .orElse(Map.of());
        GatewayRunSnapshot snapshot = new GatewayRunSnapshot(
                orchestratorId,
                orchestratorId,
                pipeline.id(),
                status,
                current.startedAt,
                Instant.now(),
                current.queued.get(),
                current.scheduled.get(),
                current.completed.get(),
                current.failed.get(),
                merge(previous, attributes == null ? new Object[]{} : attributes.entrySet().stream()
                        .flatMap(entry -> java.util.stream.Stream.of(entry.getKey(), entry.getValue()))
                        .toArray())
        );
        storage.runs().put(orchestratorId, snapshot);
    }

    private static Map<String, Object> merge(Map<String, Object> attributes, Object... values) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (attributes != null) {
            merged.putAll(attributes);
        }
        for (int index = 0; index + 1 < values.length; index += 2) {
            if (values[index + 1] != null) {
                merged.put(String.valueOf(values[index]), values[index + 1]);
            }
        }
        return merged;
    }

    private static Map<String, Object> failureMap(org.texttechnologylab.duui.exception.DUUIFailure failure) {
        if (failure == null) {
            return null;
        }
        Throwable cause = failure.cause();
        return merge(Map.of(),
                "category", failure.category().name(),
                "severity", failure.severity().name(),
                "recoverability", failure.recoverability().name(),
                "recommendedAction", failure.recommendedAction().name(),
                "stageId", failure.stageId(),
                "componentId", failure.componentId(),
                "nodeId", failure.nodeId(),
                "attempt", failure.attempt(),
                "message", failure.message(),
                "cause", cause == null ? null : cause.getClass().getName(),
                "causeMessage", cause == null ? null : cause.getMessage());
    }

    private void event(String level, String type, String subjectId, String message, Map<String, Object> attributes) {
        String id = UUID.randomUUID().toString();
        storage.events().put(id, new DUUIStoredEvent(id, Instant.now(), level, type, "duui-governor", subjectId, message, attributes));
    }

    private static final class Counters {
        private final Instant startedAt;
        private final AtomicLong queued = new AtomicLong();
        private final AtomicLong scheduled = new AtomicLong();
        private final AtomicLong completed = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();

        private Counters(Instant startedAt) {
            this.startedAt = startedAt;
        }
    }
}

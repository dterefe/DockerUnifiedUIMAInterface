package org.texttechnologylab.duui.event;

import org.texttechnologylab.duui.timelines.DUUIPhase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public record DUUIEventContext(
        DUUITraceContext trace,
        String orchestratorId,
        String taskId,
        String artifactId,
        String checkpointId,
        String stageId,
        String componentId,
        String nodeId,
        String annotatorId,
        String workerId,
        String phaseId,
        String phaseStatus,
        String phaseLifecycle
) {
    private static final ThreadLocal<DUUIPhase> CURRENT_PHASE = new ThreadLocal<>();

    public DUUIEventContext {
        trace = trace == null ? DUUITraceContext.root() : trace;
    }

    public DUUIEventContext(
            DUUITraceContext trace,
            String orchestratorId,
            String taskId,
            String artifactId,
            String checkpointId,
            String stageId,
            String componentId,
            String nodeId,
            String annotatorId,
            String workerId
    ) {
        this(trace, orchestratorId, taskId, artifactId, checkpointId, stageId, componentId, nodeId, annotatorId, workerId, null, null, null);
    }

    public static DUUIEventContext root(String orchestratorId, String taskId) {
        return new DUUIEventContext(DUUITraceContext.root(), orchestratorId, taskId, null, null, null, null, null, null, null, null, null, null);
    }

    public static void phase(DUUIPhase phase) {
        if (phase == null) {
            CURRENT_PHASE.remove();
            return;
        }
        CURRENT_PHASE.set(phase);
    }

    public static Optional<DUUIPhase> phase() {
        return Optional.ofNullable(CURRENT_PHASE.get());
    }

    public Builder toBuilder() {
        return new Builder()
                .trace(trace)
                .orchestratorId(orchestratorId)
                .taskId(taskId)
                .artifactId(artifactId)
                .checkpointId(checkpointId)
                .stageId(stageId)
                .componentId(componentId)
                .nodeId(nodeId)
                .annotatorId(annotatorId)
                .workerId(workerId)
                .phaseId(phaseId)
                .phaseStatus(phaseStatus)
                .phaseLifecycle(phaseLifecycle);
    }

    public Map<String, String> toRemoteContextMap() {
        Map<String, String> values = new LinkedHashMap<>();
        put(values, "trace_id", trace.traceId());
        put(values, "span_id", trace.spanId());
        put(values, "parent_span_id", trace.parentSpanId());
        put(values, "orchestrator_id", orchestratorId);
        put(values, "task_id", taskId);
        put(values, "artifact_id", artifactId);
        put(values, "checkpoint_id", checkpointId);
        put(values, "stage_id", stageId);
        put(values, "component_id", componentId);
        put(values, "node_id", nodeId);
        put(values, "annotator_id", annotatorId);
        put(values, "worker_id", workerId);
        put(values, "phase_id", phaseId);
        put(values, "phase_status", phaseStatus);
        put(values, "phase_lifecycle", phaseLifecycle);
        return values;
    }

    private static void put(Map<String, String> values, String key, String value) {
        if (value != null && !value.isBlank()) {
            values.put(key, value);
        }
    }

    public static final class Builder {
        private DUUITraceContext trace;
        private String orchestratorId;
        private String taskId;
        private String artifactId;
        private String checkpointId;
        private String stageId;
        private String componentId;
        private String nodeId;
        private String annotatorId;
        private String workerId;
        private String phaseId;
        private String phaseStatus;
        private String phaseLifecycle;

        public Builder trace(DUUITraceContext trace) { this.trace = trace; return this; }
        public Builder orchestratorId(String orchestratorId) { this.orchestratorId = orchestratorId; return this; }
        public Builder taskId(String taskId) { this.taskId = taskId; return this; }
        public Builder artifactId(String artifactId) { this.artifactId = artifactId; return this; }
        public Builder checkpointId(String checkpointId) { this.checkpointId = checkpointId; return this; }
        public Builder stageId(String stageId) { this.stageId = stageId; return this; }
        public Builder componentId(String componentId) { this.componentId = componentId; return this; }
        public Builder nodeId(String nodeId) { this.nodeId = nodeId; return this; }
        public Builder annotatorId(String annotatorId) { this.annotatorId = annotatorId; return this; }
        public Builder workerId(String workerId) { this.workerId = workerId; return this; }
        public Builder phaseId(String phaseId) { this.phaseId = phaseId; return this; }
        public Builder phaseStatus(String phaseStatus) { this.phaseStatus = phaseStatus; return this; }
        public Builder phaseLifecycle(String phaseLifecycle) { this.phaseLifecycle = phaseLifecycle; return this; }

        public DUUIEventContext build() {
            return new DUUIEventContext(trace, orchestratorId, taskId, artifactId, checkpointId, stageId, componentId, nodeId, annotatorId, workerId, phaseId, phaseStatus, phaseLifecycle);
        }
    }
}

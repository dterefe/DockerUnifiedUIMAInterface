package org.texttechnologylab.duui.governance;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrationResult;
import org.texttechnologylab.duui.orchestration.DUUITask;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;

import java.util.Map;

/**
 * Live inspection and control hook for DUUI orchestration.
 *
 * <p>The governor is intentionally side-effect friendly but transport agnostic:
 * gateway, tests, or embedding applications can observe and later control DUUI
 * without forcing HTTP, WebSocket, or a database into the orchestrator core.</p>
 */
public interface DUUIGovernor {
    DUUIGovernor NONE = new DUUIGovernor() {
    };

    default void onRunStarted(String orchestratorId, DUUIPipeline pipeline, Map<String, Object> attributes) {
    }

    default void onArtifactQueued(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, Map<String, Object> attributes) {
    }

    default void onTaskScheduled(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, DUUITask<?> task, Map<String, Object> attributes) {
    }

    default void onTaskCompleted(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, DUUIExecutionResult<?> result, Map<String, Object> attributes) {
    }

    default void onTaskFailed(String orchestratorId, DUUIPipeline pipeline, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, Throwable error, Map<String, Object> attributes) {
    }

    default void onRunCompleted(String orchestratorId, DUUIPipeline pipeline, DUUIOrchestrationResult result, Map<String, Object> attributes) {
    }
}

package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.Map;

public record GatewayRunSnapshot(
        String id,
        String orchestratorId,
        String pipelineId,
        String status,
        Instant startedAt,
        Instant updatedAt,
        long queuedArtifacts,
        long scheduledTasks,
        long completedTasks,
        long failedTasks,
        Map<String, Object> attributes
) {
    public GatewayRunSnapshot {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}

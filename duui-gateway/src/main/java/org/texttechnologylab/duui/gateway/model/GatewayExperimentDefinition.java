package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record GatewayExperimentDefinition(
        String id,
        String name,
        String pipelineId,
        Map<String, Object> componentConfiguration,
        Map<String, Object> execution,
        Map<String, Object> flow,
        Map<String, Object> scheduling,
        List<String> subExperiments,
        Instant createdAt,
        Instant updatedAt
) {
    public GatewayExperimentDefinition {
        componentConfiguration = componentConfiguration == null ? Map.of() : Map.copyOf(componentConfiguration);
        execution = execution == null ? Map.of() : Map.copyOf(execution);
        flow = flow == null ? Map.of() : Map.copyOf(flow);
        scheduling = scheduling == null ? Map.of() : Map.copyOf(scheduling);
        subExperiments = subExperiments == null ? List.of() : List.copyOf(subExperiments);
    }
}

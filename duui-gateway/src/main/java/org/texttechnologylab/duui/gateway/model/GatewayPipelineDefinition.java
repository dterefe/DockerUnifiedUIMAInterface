package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record GatewayPipelineDefinition(
        String id,
        String name,
        List<String> componentIds,
        List<PipelineEdge> edges,
        Map<String, Object> structure,
        List<String> tags,
        Instant createdAt,
        Instant updatedAt
) {
    public GatewayPipelineDefinition {
        componentIds = componentIds == null ? List.of() : List.copyOf(componentIds);
        edges = edges == null ? List.of() : List.copyOf(edges);
        structure = structure == null ? Map.of() : Map.copyOf(structure);
        tags = tags == null ? List.of() : List.copyOf(tags);
    }

    public record PipelineEdge(String from, String to, String checkpoint) {
    }
}

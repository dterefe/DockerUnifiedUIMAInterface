package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record GatewayComponentDefinition(
        String id,
        String name,
        String annotatorId,
        String driver,
        String environment,
        Map<String, String> parameters,
        Map<String, Object> deployment,
        String sourceView,
        String targetView,
        List<String> tags,
        Instant createdAt,
        Instant updatedAt
) {
    public GatewayComponentDefinition {
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        deployment = deployment == null ? Map.of() : Map.copyOf(deployment);
        tags = tags == null ? List.of() : List.copyOf(tags);
    }
}

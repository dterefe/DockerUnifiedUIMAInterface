package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record GatewayServiceDefinition(
        String id,
        String name,
        String kind,
        String environment,
        String image,
        String endpoint,
        String status,
        int scale,
        int workers,
        Map<String, String> parameters,
        Map<String, Object> deployment,
        List<String> endpoints,
        List<String> tags,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Map<String, Object> runtime
) {
    public GatewayServiceDefinition {
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        deployment = deployment == null ? Map.of() : Map.copyOf(deployment);
        endpoints = endpoints == null ? List.of() : List.copyOf(endpoints);
        tags = tags == null ? List.of() : List.copyOf(tags);
        runtime = runtime == null ? Map.of() : Map.copyOf(runtime);
        scale = Math.max(1, scale);
        workers = Math.max(1, workers);
    }
}

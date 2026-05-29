package org.texttechnologylab.duui.gateway.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record GatewayAnnotatorRegistration(
        String id,
        String name,
        String endpoint,
        String environment,
        String image,
        String status,
        Map<String, Object> descriptor,
        Instant validatedAt,
        List<String> errors,
        List<String> tags
) {
    public GatewayAnnotatorRegistration {
        descriptor = descriptor == null ? Map.of() : Map.copyOf(descriptor);
        errors = errors == null ? List.of() : List.copyOf(errors);
        tags = tags == null ? List.of() : List.copyOf(tags);
    }
}

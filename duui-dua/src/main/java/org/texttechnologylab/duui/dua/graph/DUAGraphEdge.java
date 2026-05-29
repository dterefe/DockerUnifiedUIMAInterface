package org.texttechnologylab.duui.dua.graph;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;

public record DUAGraphEdge(String id, String label, String source, String target, Map<String, Object> properties) {
    public DUAGraphEdge {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        properties = immutableProperties(properties);
    }

    public DUAGraphEdge with(String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(properties);
        next.put(key, value);
        return new DUAGraphEdge(id, label, source, target, next);
    }

    private static Map<String, Object> immutableProperties(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (key != null && value != null) {
                sanitized.put(key, value);
            }
        });
        return sanitized.isEmpty() ? Map.of() : Collections.unmodifiableMap(sanitized);
    }
}

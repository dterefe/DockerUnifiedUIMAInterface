package org.texttechnologylab.duui.dua.graph;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;

public record DUAGraphNode(String id, String label, Map<String, Object> properties) {
    public DUAGraphNode {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(label, "label");
        properties = immutableProperties(properties);
    }

    public static DUAGraphNode of(String id, String label) {
        return new DUAGraphNode(id, label, Map.of());
    }

    public DUAGraphNode with(String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(properties);
        next.put(key, value);
        return new DUAGraphNode(id, label, next);
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

package org.texttechnologylab.duui.protocol.v1;

import org.texttechnologylab.duui.event.DUUIEventSink;

import java.util.List;

public record DUUIV1TelemetryConfig(
        boolean enabled,
        int ttlMinutes,
        DUUIEventSink sink,
        List<String> resource,
        List<String> stats,
        List<String> scopes,
        int sampleIntervalMs
) {
    public DUUIV1TelemetryConfig(boolean enabled, int ttlMinutes, DUUIEventSink sink) {
        this(
                enabled,
                ttlMinutes,
                sink,
                List.of("cpu", "memory"),
                List.of("duration", "throughput", "histogram"),
                List.of("global", "component", "component_replica", "request"),
                500
        );
    }

    public DUUIV1TelemetryConfig {
        ttlMinutes = Math.max(1, Math.min(60, ttlMinutes));
        resource = resource == null ? List.of() : clean(resource);
        stats = stats == null ? List.of() : clean(stats);
        scopes = scopes == null ? List.of() : clean(scopes);
        sampleIntervalMs = Math.max(100, sampleIntervalMs);
    }

    public static DUUIV1TelemetryConfig disabled() {
        return new DUUIV1TelemetryConfig(false, 5, null);
    }

    public static DUUIV1TelemetryConfig enabled(DUUIEventSink sink) {
        return new DUUIV1TelemetryConfig(true, 5, sink);
    }

    private static List<String> clean(List<String> values) {
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
    }
}

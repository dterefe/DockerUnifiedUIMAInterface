package org.texttechnologylab.duui.protocol.v1;

import java.util.Map;

public record DUUIV1Config(
    int concurrency,
    String sourceView,
    String targetView,
    Map<String, String> parameters,
    DUUIV1TelemetryConfig telemetry,
    boolean streamingTransport,
    String contentType
) {
    public DUUIV1Config(int concurrency, String sourceView, String targetView, Map<String, String> parameters) {
        this(concurrency, sourceView, targetView, parameters, DUUIV1TelemetryConfig.disabled());
    }

    public DUUIV1Config(int concurrency, String sourceView, String targetView, Map<String, String> parameters, DUUIV1TelemetryConfig telemetry) {
        this(concurrency, sourceView, targetView, parameters, telemetry, false, "application/octet-stream");
    }

    public DUUIV1Config {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be greater than 0");
        }
        sourceView = sourceView == null ? "_InitialView" : sourceView;
        targetView = targetView == null ? "_InitialView" : targetView;
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        telemetry = telemetry == null ? DUUIV1TelemetryConfig.disabled() : telemetry;
        contentType = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    }
}

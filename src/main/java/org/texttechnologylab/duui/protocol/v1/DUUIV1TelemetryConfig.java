package org.texttechnologylab.duui.protocol.v1;

import org.texttechnologylab.duui.event.DUUIEventSink;

public record DUUIV1TelemetryConfig(boolean enabled, int ttlMinutes, DUUIEventSink sink) {
    public DUUIV1TelemetryConfig {
        ttlMinutes = Math.max(1, Math.min(60, ttlMinutes));
    }

    public static DUUIV1TelemetryConfig disabled() {
        return new DUUIV1TelemetryConfig(false, 5, null);
    }

    public static DUUIV1TelemetryConfig enabled(DUUIEventSink sink) {
        return new DUUIV1TelemetryConfig(true, 5, sink);
    }
}

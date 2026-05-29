package org.texttechnologylab.duui.event;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Objects;

public record DUUITraceContext(String traceId, String spanId, String parentSpanId) {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final HexFormat HEX = HexFormat.of();

    public DUUITraceContext {
        traceId = normalize(traceId, 16);
        spanId = normalize(spanId, 8);
        parentSpanId = parentSpanId == null || parentSpanId.isBlank() ? null : normalize(parentSpanId, 8);
    }

    public static DUUITraceContext root() {
        return new DUUITraceContext(randomHex(16), randomHex(8), null);
    }

    public DUUITraceContext child() {
        return new DUUITraceContext(traceId, randomHex(8), spanId);
    }

    private static String normalize(String value, int bytes) {
        Objects.requireNonNull(value, "trace value");
        String normalized = value.replace("-", "").toLowerCase();
        int expected = bytes * 2;
        if (normalized.length() != expected || !normalized.matches("[0-9a-f]+")) {
            throw new IllegalArgumentException("Trace value must be " + expected + " lowercase hex characters.");
        }
        return normalized;
    }

    private static String randomHex(int bytes) {
        byte[] value = new byte[bytes];
        RANDOM.nextBytes(value);
        return HEX.formatHex(value);
    }
}

package org.texttechnologylab.duui.dua;

import java.util.Objects;

public record DUAArtifact<T>(String id, T payload) {
    public DUAArtifact {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
    }

    public static <T> DUAArtifact<T> of(T payload) {
        return new DUAArtifact<>(DUAId.create().value(), payload);
    }

    public static <T> DUAArtifact<T> of(String id, T payload) {
        return new DUAArtifact<>(id, payload);
    }
}

package org.texttechnologylab.duui.artifact;

import java.util.Objects;

public final class DUUIArtifactType<T> {
    private final String id;

    private DUUIArtifactType(String id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    public static <T> DUUIArtifactType<T> of(String id) {
        return new DUUIArtifactType<>(id);
    }

    public static <T> DUUIArtifactType<T> javaType(Class<T> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType");
        return of(payloadType.getName());
    }

    public String id() {
        return id;
    }

    public boolean accepts(DUUIArtifactType<?> other) {
        return other != null && id.equals(other.id);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DUUIArtifactType<?> artifactType && id.equals(artifactType.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }
}

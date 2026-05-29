package org.texttechnologylab.duui.dua.model;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAPipelineArtifact(DUAId id, String componentName, String componentVersion,
                                  Instant createdAt, Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAPipelineArtifact {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(componentName, "componentName");
        componentVersion = componentVersion == null ? "" : componentVersion;
        createdAt = createdAt == null ? Instant.now() : createdAt;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.PIPELINE_ARTIFACT;
    }
}

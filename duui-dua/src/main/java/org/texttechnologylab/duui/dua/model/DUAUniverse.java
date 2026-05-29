package org.texttechnologylab.duui.dua.model;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAUniverse(DUAId id, String name, Instant createdAt, Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAUniverse {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        createdAt = createdAt == null ? Instant.now() : createdAt;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.UNIVERSE;
    }
}

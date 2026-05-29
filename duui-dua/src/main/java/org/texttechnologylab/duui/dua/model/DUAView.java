package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAView(DUAId id, DUAId ownerId, String name, DUAScope scope, Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAView {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(ownerId, "ownerId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(scope, "scope");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.VIEW;
    }
}

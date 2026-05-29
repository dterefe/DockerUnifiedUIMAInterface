package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAType(DUAId id, String name, String supertypeName, Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAType {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        supertypeName = supertypeName == null ? "" : supertypeName;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.TYPE;
    }
}

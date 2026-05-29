package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAFeature(DUAId id, String ownerTypeName, String name, String rangeTypeName,
                         Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAFeature {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(ownerTypeName, "ownerTypeName");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(rangeTypeName, "rangeTypeName");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.FEATURE;
    }
}

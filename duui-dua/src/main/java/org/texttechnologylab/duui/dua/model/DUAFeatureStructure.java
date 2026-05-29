package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAFeatureStructure(DUAId id, String typeName, DUAScope scope,
                                  Map<DUAFeatureKey, DUAValue> features) implements DUAEntity {
    public DUAFeatureStructure {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(scope, "scope");
        features = features == null ? Map.of() : Map.copyOf(features);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.FEATURE_STRUCTURE;
    }
}

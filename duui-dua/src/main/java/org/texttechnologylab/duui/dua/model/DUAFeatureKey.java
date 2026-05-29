package org.texttechnologylab.duui.dua.model;

import java.util.Objects;

public record DUAFeatureKey(String ownerTypeName, String featureName) {
    public DUAFeatureKey {
        Objects.requireNonNull(ownerTypeName, "ownerTypeName");
        Objects.requireNonNull(featureName, "featureName");
    }

    public String qualifiedName() {
        return ownerTypeName + "#" + featureName;
    }
}

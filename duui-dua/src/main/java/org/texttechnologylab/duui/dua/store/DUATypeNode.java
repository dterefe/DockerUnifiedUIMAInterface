package org.texttechnologylab.duui.dua.store;

import java.util.Objects;
import java.util.OptionalInt;

public record DUATypeNode(int typeId, String typeName, OptionalInt parentTypeId) {
    public DUATypeNode {
        if (typeId < 0) {
            throw new IllegalArgumentException("typeId must not be negative");
        }
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(parentTypeId, "parentTypeId");
    }
}

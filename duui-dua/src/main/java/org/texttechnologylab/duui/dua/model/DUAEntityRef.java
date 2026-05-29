package org.texttechnologylab.duui.dua.model;

import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAEntityRef<T extends DUAEntity>(DUAId id, DUAEntityKind kind) {
    public DUAEntityRef {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
    }

    public static <T extends DUAEntity> DUAEntityRef<T> of(T entity) {
        return new DUAEntityRef<>(entity.id(), entity.kind());
    }
}

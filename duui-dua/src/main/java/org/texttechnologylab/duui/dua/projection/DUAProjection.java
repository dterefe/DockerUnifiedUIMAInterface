package org.texttechnologylab.duui.dua.projection;

import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAProjection<T>(DUAId id, DUAProjectionType<T> type) {
    public DUAProjection {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(type, "type");
    }

    public static <T> DUAProjection<T> create(DUAProjectionType<T> type) {
        return new DUAProjection<>(DUAId.create(), type);
    }

    public String typeName() {
        return type.typeName();
    }

    public Class<T> markerClass() {
        return type.markerClass();
    }

    public String modeName() {
        return type.modeName();
    }
}

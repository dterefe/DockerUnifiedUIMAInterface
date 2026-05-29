package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUASofa(DUAId id, DUAId viewId, String mimeType, DUAEntityRef<? extends DUAEntity> payload,
                      Map<String, DUAValue> metadata) implements DUAEntity {
    public DUASofa {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(viewId, "viewId");
        Objects.requireNonNull(mimeType, "mimeType");
        Objects.requireNonNull(payload, "payload");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.SOFA;
    }
}

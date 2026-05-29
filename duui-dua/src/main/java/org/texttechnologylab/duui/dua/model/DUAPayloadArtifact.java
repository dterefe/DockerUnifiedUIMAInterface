package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAPayloadArtifact(DUAId id, String mediaType, String storagePath, long byteLength,
                                 Map<String, DUAValue> metadata) implements DUAEntity {
    public DUAPayloadArtifact {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(mediaType, "mediaType");
        Objects.requireNonNull(storagePath, "storagePath");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.PAYLOAD_ARTIFACT;
    }
}

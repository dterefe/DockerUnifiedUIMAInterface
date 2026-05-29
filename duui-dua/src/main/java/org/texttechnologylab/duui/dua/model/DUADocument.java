package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUADocument(DUAId id, DUAId corpusId, String externalId, Map<String, DUAValue> metadata) implements DUAEntity {
    public DUADocument {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(corpusId, "corpusId");
        externalId = externalId == null ? id.value() : externalId;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.DOCUMENT;
    }
}

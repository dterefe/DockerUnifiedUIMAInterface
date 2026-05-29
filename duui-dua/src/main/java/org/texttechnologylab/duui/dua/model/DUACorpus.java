package org.texttechnologylab.duui.dua.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUACorpus(DUAId id, DUAId universeId, String name, List<DUAEntityRef<DUADocument>> documents,
                        Map<String, DUAValue> metadata) implements DUAEntity {
    public DUACorpus {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(universeId, "universeId");
        Objects.requireNonNull(name, "name");
        documents = documents == null ? List.of() : List.copyOf(documents);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.CORPUS;
    }
}

package org.texttechnologylab.duui.dua.model;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.texttechnologylab.duui.dua.DUAId;

public record DUADomainUnit(DUAId id, String name, Optional<URI> uri, DUAScope scope,
                            DUAEntityRef<? extends DUAEntity> subject,
                            Map<String, DUAValue> metadata) implements DUAEntity {
    public DUADomainUnit {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        uri = uri == null ? Optional.empty() : uri;
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(subject, "subject");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityKind kind() {
        return DUAEntityKind.DOMAIN_UNIT;
    }
}

package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAReferenceAssociation(DUAId id, String name, DUAEntityRef<DUADomainUnit> context,
                                      DUAEntityRef<DUADomainUnit> referent, String role,
                                      Map<String, DUAValue> metadata) implements DUAAssociation {
    public DUAReferenceAssociation {
        Objects.requireNonNull(id, "id");
        name = name == null ? "reference" : name;
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(referent, "referent");
        role = role == null ? "" : role;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityRef<DUADomainUnit> source() {
        return context;
    }

    @Override
    public DUAEntityRef<DUADomainUnit> target() {
        return referent;
    }
}

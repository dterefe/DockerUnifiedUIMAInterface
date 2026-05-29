package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAEquivalenceAssociation(DUAId id, String name, DUAEntityRef<DUADomainUnit> source,
                                        DUAEntityRef<DUADomainUnit> target, String basis,
                                        Map<String, DUAValue> metadata) implements DUAAssociation {
    public DUAEquivalenceAssociation {
        Objects.requireNonNull(id, "id");
        name = name == null ? "equivalence" : name;
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        basis = basis == null ? "" : basis;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}

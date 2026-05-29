package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import org.texttechnologylab.duui.dua.DUAId;

public record DUASequenceAssociation(DUAId id, String name, DUAEntityRef<DUADomainUnit> previous,
                                     DUAEntityRef<DUADomainUnit> next, OptionalInt order,
                                     Map<String, DUAValue> metadata) implements DUAAssociation {
    public DUASequenceAssociation {
        Objects.requireNonNull(id, "id");
        name = name == null ? "sequence" : name;
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(next, "next");
        order = order == null ? OptionalInt.empty() : order;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityRef<DUADomainUnit> source() {
        return previous;
    }

    @Override
    public DUAEntityRef<DUADomainUnit> target() {
        return next;
    }
}

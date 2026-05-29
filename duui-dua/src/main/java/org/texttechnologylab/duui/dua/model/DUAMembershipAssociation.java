package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAMembershipAssociation(DUAId id, String name, DUAEntityRef<DUADomainUnit> whole,
                                       DUAEntityRef<DUADomainUnit> part, OptionalInt order,
                                       Map<String, DUAValue> metadata) implements DUAAssociation {
    public DUAMembershipAssociation {
        Objects.requireNonNull(id, "id");
        name = name == null ? "membership" : name;
        Objects.requireNonNull(whole, "whole");
        Objects.requireNonNull(part, "part");
        order = order == null ? OptionalInt.empty() : order;
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    @Override
    public DUAEntityRef<DUADomainUnit> source() {
        return whole;
    }

    @Override
    public DUAEntityRef<DUADomainUnit> target() {
        return part;
    }
}

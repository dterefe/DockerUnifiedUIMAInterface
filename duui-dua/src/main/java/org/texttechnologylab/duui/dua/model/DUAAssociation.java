package org.texttechnologylab.duui.dua.model;

import java.util.Map;
import org.texttechnologylab.duui.dua.DUAId;

public sealed interface DUAAssociation extends DUAEntity permits DUAEquivalenceAssociation,
        DUAMembershipAssociation, DUAReferenceAssociation, DUASequenceAssociation {
    DUAEntityRef<DUADomainUnit> source();

    DUAEntityRef<DUADomainUnit> target();

    String name();

    Map<String, DUAValue> metadata();

    @Override
    default DUAEntityKind kind() {
        return DUAEntityKind.ASSOCIATION;
    }

    static DUAEquivalenceAssociation equivalence(DUAId id, DUAEntityRef<DUADomainUnit> one,
                                                 DUAEntityRef<DUADomainUnit> other, String basis) {
        return new DUAEquivalenceAssociation(id, "equivalence", one, other, basis, Map.of());
    }
}

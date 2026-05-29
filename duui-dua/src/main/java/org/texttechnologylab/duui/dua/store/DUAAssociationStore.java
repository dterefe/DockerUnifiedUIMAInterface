package org.texttechnologylab.duui.dua.store;

import java.util.Optional;
import java.util.stream.Stream;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.model.DUAAssociation;
import org.texttechnologylab.duui.dua.model.DUAAssociationType;
import org.texttechnologylab.duui.dua.model.DUADomainUnit;
import org.texttechnologylab.duui.dua.model.DUAEntityRef;

public interface DUAAssociationStore {
    Optional<DUAAssociation> read(DUAId id);

    DUAWriteResult write(DUAAssociation association);

    Stream<DUAAssociation> outgoing(DUAEntityRef<DUADomainUnit> source, DUAAssociationType type);

    Stream<DUAAssociation> incoming(DUAEntityRef<DUADomainUnit> target, DUAAssociationType type);
}

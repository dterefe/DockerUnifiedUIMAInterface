package org.texttechnologylab.duui.dua.model;

public enum DUAAssociationType {
    EQUIVALENCE,
    MEMBERSHIP,
    REFERENCE,
    SEQUENCE;

    public static DUAAssociationType of(DUAAssociation association) {
        if (association instanceof DUAEquivalenceAssociation) {
            return EQUIVALENCE;
        }
        if (association instanceof DUAMembershipAssociation) {
            return MEMBERSHIP;
        }
        if (association instanceof DUAReferenceAssociation) {
            return REFERENCE;
        }
        if (association instanceof DUASequenceAssociation) {
            return SEQUENCE;
        }
        throw new IllegalArgumentException("Unsupported association class: " + association.getClass().getName());
    }
}

package org.texttechnologylab.duui.dua.query;

import java.util.Objects;

public sealed interface DUATypeQuery permits
        DUATypeQuery.ExactType,
        DUATypeQuery.Subtypes,
        DUATypeQuery.Supertypes,
        DUATypeQuery.ReferenceTraversal,
        DUATypeQuery.OutgoingReferences {

    record ExactType(String typeName) implements DUATypeQuery {
        public ExactType {
            Objects.requireNonNull(typeName, "typeName");
        }
    }

    record Subtypes(String typeName, boolean transitive) implements DUATypeQuery {
        public Subtypes {
            Objects.requireNonNull(typeName, "typeName");
        }
    }

    record Supertypes(String typeName, boolean transitive) implements DUATypeQuery {
        public Supertypes {
            Objects.requireNonNull(typeName, "typeName");
        }
    }

    /**
     * Traverses reverse references: finds all source feature structures that
     * reference the given {@code targetFsRef} via the given {@code featureCode}.
     */
    record ReferenceTraversal(long targetFsRef, int featureCode) implements DUATypeQuery {
        public ReferenceTraversal {
            if (targetFsRef < 0) {
                throw new IllegalArgumentException("targetFsRef must not be negative");
            }
            if (featureCode < 0) {
                throw new IllegalArgumentException("featureCode must not be negative");
            }
        }
    }

    /**
     * Traverses forward references: finds all target feature structures that
     * are referenced by the given {@code sourceFsRef} via the given {@code featureCode}.
     */
    record OutgoingReferences(long sourceFsRef, int featureCode) implements DUATypeQuery {
        public OutgoingReferences {
            if (sourceFsRef < 0) {
                throw new IllegalArgumentException("sourceFsRef must not be negative");
            }
            if (featureCode < 0) {
                throw new IllegalArgumentException("featureCode must not be negative");
            }
        }
    }
}

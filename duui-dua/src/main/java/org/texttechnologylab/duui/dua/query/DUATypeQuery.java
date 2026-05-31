package org.texttechnologylab.duui.dua.query;

import java.util.Objects;

public sealed interface DUATypeQuery permits
        DUATypeQuery.ExactType,
        DUATypeQuery.Subtypes,
        DUATypeQuery.Supertypes,
        DUATypeQuery.ReferenceTraversal {

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

    record ReferenceTraversal(String sourceTypeName, String featureName, String targetTypeName)
            implements DUATypeQuery {
        public ReferenceTraversal {
            Objects.requireNonNull(sourceTypeName, "sourceTypeName");
            Objects.requireNonNull(featureName, "featureName");
            Objects.requireNonNull(targetTypeName, "targetTypeName");
        }
    }
}

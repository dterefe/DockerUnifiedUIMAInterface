package org.texttechnologylab.duui.dua.query;

import java.util.Objects;
import java.util.OptionalInt;

public sealed interface DUATextQuery permits
        DUATextQuery.Exact,
        DUATextQuery.Substring,
        DUATextQuery.CoveredText {

    record Exact(long sofaFsRef, String text) implements DUATextQuery {
        public Exact {
            requireSofa(sofaFsRef);
            Objects.requireNonNull(text, "text");
        }
    }

    record Substring(long sofaFsRef, String text) implements DUATextQuery {
        public Substring {
            requireSofa(sofaFsRef);
            if (text == null || text.isBlank()) {
                throw new IllegalArgumentException("text must not be blank");
            }
        }
    }

    record CoveredText(long sofaFsRef, OptionalInt typeId, String text) implements DUATextQuery {
        public CoveredText {
            requireSofa(sofaFsRef);
            Objects.requireNonNull(typeId, "typeId");
            Objects.requireNonNull(text, "text");
        }
    }

    private static void requireSofa(long sofaFsRef) {
        if (sofaFsRef < 0) {
            throw new IllegalArgumentException("sofaFsRef must not be negative");
        }
    }
}

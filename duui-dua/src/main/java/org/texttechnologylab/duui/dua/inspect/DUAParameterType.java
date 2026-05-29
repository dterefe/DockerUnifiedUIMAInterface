package org.texttechnologylab.duui.dua.inspect;

import java.util.List;
import java.util.Objects;
import org.texttechnologylab.duui.dua.model.DUAEntityKind;

public sealed interface DUAParameterType permits DUAParameterType.Text, DUAParameterType.Number,
        DUAParameterType.Boolean, DUAParameterType.Entity, DUAParameterType.Feature,
        DUAParameterType.ListOf {
    record Text() implements DUAParameterType {
    }

    record Number() implements DUAParameterType {
    }

    record Boolean() implements DUAParameterType {
    }

    record Entity(DUAEntityKind kind) implements DUAParameterType {
        public Entity {
            Objects.requireNonNull(kind, "kind");
        }
    }

    record Feature(String rangeTypeName) implements DUAParameterType {
        public Feature {
            Objects.requireNonNull(rangeTypeName, "rangeTypeName");
        }
    }

    record ListOf(DUAParameterType elementType) implements DUAParameterType {
        public ListOf {
            Objects.requireNonNull(elementType, "elementType");
        }
    }

    static List<DUAParameterType> scalarTypes() {
        return List.of(new Text(), new Number(), new Boolean());
    }
}

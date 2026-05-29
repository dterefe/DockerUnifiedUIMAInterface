package org.texttechnologylab.duui.dua.inspect;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.model.DUAValue;

public record DUAInspectorComponent(DUAId id, DUAInspectorComponentKind kind, String name,
                                    List<DUAComponentParameter> parameters,
                                    DUADataBinding binding,
                                    Map<String, DUAValue> metadata) {
    public DUAInspectorComponent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(name, "name");
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        Objects.requireNonNull(binding, "binding");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}

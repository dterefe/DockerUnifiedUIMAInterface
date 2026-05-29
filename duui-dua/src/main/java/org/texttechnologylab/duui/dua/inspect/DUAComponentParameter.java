package org.texttechnologylab.duui.dua.inspect;

import java.util.Objects;
import java.util.Optional;
import org.texttechnologylab.duui.dua.model.DUAValue;

public record DUAComponentParameter(String name, DUAParameterType type, boolean required,
                                    Optional<DUAValue> defaultValue) {
    public DUAComponentParameter {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        defaultValue = defaultValue == null ? Optional.empty() : defaultValue;
    }
}

package org.texttechnologylab.duui.dua.projection;

import java.util.Objects;

public record DUAProjectionType<T>(String typeName, Class<T> markerClass) {
    public DUAProjectionType {
        typeName = requireText(typeName, "typeName");
        Objects.requireNonNull(markerClass, "markerClass");
    }

    public static <T> DUAProjectionType<T> of(String typeName, Class<T> markerClass) {
        return new DUAProjectionType<>(typeName, markerClass);
    }

    public String markerName() {
        return markerClass.getSimpleName();
    }

    public String modeName() {
        return "JDUA<" + markerName() + ">";
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }
}

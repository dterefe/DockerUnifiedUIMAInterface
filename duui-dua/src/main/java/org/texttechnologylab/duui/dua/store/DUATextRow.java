package org.texttechnologylab.duui.dua.store;

import java.util.Objects;

public record DUATextRow(long sofaFsRef, long fsRef, String role, String text) {
    public DUATextRow {
        if (sofaFsRef < 0) {
            throw new IllegalArgumentException("sofaFsRef must not be negative");
        }
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(text, "text");
        if (fsRef < 0) {
            throw new IllegalArgumentException("fsRef must not be negative");
        }
    }
}

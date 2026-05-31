package org.texttechnologylab.duui.dua.query;

import java.util.Optional;

public record DUAAnnotationSpan(
        long sofaFsRef,
        long fsRef,
        int typeId,
        int begin,
        int end,
        Optional<String> coveredText
) {
    public DUAAnnotationSpan {
        if (sofaFsRef < 0) {
            throw new IllegalArgumentException("sofaFsRef must not be negative");
        }
        if (fsRef < 0) {
            throw new IllegalArgumentException("fsRef must not be negative");
        }
        if (typeId < 0) {
            throw new IllegalArgumentException("typeId must not be negative");
        }
        if (begin < 0) {
            throw new IllegalArgumentException("begin must not be negative");
        }
        if (end < begin) {
            throw new IllegalArgumentException("end must be greater than or equal to begin");
        }
        coveredText = coveredText == null ? Optional.empty() : coveredText;
    }
}

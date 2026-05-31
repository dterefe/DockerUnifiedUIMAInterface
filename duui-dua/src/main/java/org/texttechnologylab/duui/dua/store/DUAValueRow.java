package org.texttechnologylab.duui.dua.store;

import java.util.Objects;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;

public record DUAValueRow(
        DUAId casId,
        DUAId viewId,
        long fsRef,
        int featureId,
        String featureName,
        DUACasValue value
) {
    public DUAValueRow {
        Objects.requireNonNull(casId, "casId");
        Objects.requireNonNull(viewId, "viewId");
        Objects.requireNonNull(featureName, "featureName");
        Objects.requireNonNull(value, "value");
        if (fsRef < 0) {
            throw new IllegalArgumentException("fsRef must not be negative");
        }
        if (featureId < 0) {
            throw new IllegalArgumentException("featureId must not be negative");
        }
    }
}

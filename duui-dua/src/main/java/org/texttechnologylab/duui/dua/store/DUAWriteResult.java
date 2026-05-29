package org.texttechnologylab.duui.dua.store;

import java.util.Objects;
import org.texttechnologylab.duui.dua.DUAId;

public record DUAWriteResult(DUAId entityId, DUARevision revision) {
    public DUAWriteResult {
        Objects.requireNonNull(entityId, "entityId");
        Objects.requireNonNull(revision, "revision");
    }
}

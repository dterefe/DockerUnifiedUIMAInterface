package org.texttechnologylab.duui.dua.store;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.model.DUAPayloadArtifact;

public interface DUAPayloadStore {
    Optional<DUAPayloadArtifact> describe(DUAId id);

    InputStream read(DUAId id);

    DUAWriteResult write(DUAPayloadArtifact artifact, DUAOperation<InputStream> payload);

    void copyTo(DUAId id, OutputStream output);
}

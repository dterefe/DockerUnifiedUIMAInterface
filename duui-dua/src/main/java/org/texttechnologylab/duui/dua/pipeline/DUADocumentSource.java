package org.texttechnologylab.duui.dua.pipeline;

import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.DUAArtifact;
import org.texttechnologylab.duui.dua.DUAEmitter;
import org.texttechnologylab.duui.dua.DUASource;

import java.nio.file.Path;
import java.util.Objects;

public final class DUADocumentSource implements DUASource<DUADocumentRef> {
    private final Path archive;

    public DUADocumentSource(Path archive) {
        this.archive = Objects.requireNonNull(archive, "archive");
    }

    @Override
    public void generate(DUAEmitter<DUADocumentRef> emitter) throws Exception {
        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            for (var artifact : reader.manifest().getArtifacts()) {
                if ("cas-xmi".equals(artifact.kind())) {
                    emitter.emit(DUAArtifact.of(new DUADocumentRef(archive, artifact.id())));
                }
            }
        }
    }
}

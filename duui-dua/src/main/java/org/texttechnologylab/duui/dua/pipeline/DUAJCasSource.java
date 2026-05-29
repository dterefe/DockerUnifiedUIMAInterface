package org.texttechnologylab.duui.dua.pipeline;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.dua.DUAArtifact;
import org.texttechnologylab.duui.dua.DUAEmitter;
import org.texttechnologylab.duui.dua.DUASource;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;

import java.nio.file.Path;
import java.util.Objects;

public final class DUAJCasSource implements DUASource<JCas> {
    private final Path archive;
    private final DUAXmiBridge bridge = new DUAXmiBridge();

    public DUAJCasSource(Path archive) {
        this.archive = Objects.requireNonNull(archive, "archive");
    }

    @Override
    public void generate(DUAEmitter<JCas> emitter) throws Exception {
        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            for (var artifact : reader.manifest().getArtifacts()) {
                if ("cas-xmi".equals(artifact.kind())) {
                    emitter.emit(DUAArtifact.of(artifact.id(), bridge.materialize(reader, artifact.id())));
                }
            }
        }
    }
}

package org.texttechnologylab.duui.dua.pipeline;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.dua.DUAArtifact;
import org.texttechnologylab.duui.dua.DUATarget;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;
import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public final class DUAJCasTarget implements DUATarget<JCas>, AutoCloseable {
    private final String corpusId;
    private final DUAGraphCodec codec;
    private final DUAXmiBridge bridge = new DUAXmiBridge();
    private final DUAArchiveWriter writer;

    public DUAJCasTarget(Path archive, String corpusId) throws IOException {
        this(archive, corpusId, new DUAJsonlGraphCodec());
    }

    public DUAJCasTarget(Path archive, String corpusId, DUAGraphCodec codec) throws IOException {
        this.corpusId = Objects.requireNonNull(corpusId, "corpusId");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.writer = DUAArchiveWriter.create(archive);
    }

    @Override
    public void accept(DUAArtifact<JCas> artifact) throws Exception {
        bridge.addJCas(artifact.payload(), writer, corpusId, artifact.id(), codec);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}

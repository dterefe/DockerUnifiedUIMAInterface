package org.texttechnologylab.duui.dua;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DUAXmiBridgeTest {
    @TempDir
    Path temp;

    @Test
    void storesAndMaterializesJCasThroughDua() throws Exception {
        Path archive = temp.resolve("cas.dua");
        DUAXmiBridge bridge = new DUAXmiBridge();
        JCas source = JCasFactory.createJCas();
        source.setDocumentLanguage("en");
        source.setDocumentText("DUA carries CAS as one artifact family.");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-cas")) {
            bridge.addJCas(source, writer, "corpus-1", "doc-1", new DUAJsonlGraphCodec());
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            JCas restored = bridge.materialize(reader, "doc-1");
            assertEquals(source.getDocumentText(), restored.getDocumentText());
            assertEquals("en", restored.getDocumentLanguage());
            assertEquals(1, reader.manifest().getPartitions().size());
        }
    }
}

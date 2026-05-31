package org.texttechnologylab.duui.pipeline.io.document;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.storage.DUUILocalStorageClient;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUUIXmiDocumentAdaptersTest {
    @TempDir
    Path temp;

    @Test
    void xmiRoundTripsThroughDocumentClient() throws Exception {
        DUUILocalStorageClient client = new DUUILocalStorageClient(temp);
        DUUIAddress output = new DUUIAddress("local", "directory", "out", null, null);
        DUUIAddress source = new DUUIAddress("local", "directory", "out", null, null);

        JCas cas = JCasFactory.createJCas();
        cas.setDocumentText("Shared document adapter text.");

        DUUIXmiDocumentWriter.builder()
                .client(client)
                .output(output)
                .fileName(artifact -> "doc.xmi")
                .build()
                .accept(DUUIArtifact.of(cas));

        assertTrue(temp.resolve("out/doc.xmi").toFile().isFile());

        List<JCas> read = new ArrayList<>();
        DUUIXmiDocumentReader.builder()
                .client(client)
                .source(source)
                .build()
                .generate(artifact -> read.add(artifact.payload()));

        assertEquals(1, read.size());
        assertEquals(cas.getDocumentText(), read.getFirst().getDocumentText());
    }
}

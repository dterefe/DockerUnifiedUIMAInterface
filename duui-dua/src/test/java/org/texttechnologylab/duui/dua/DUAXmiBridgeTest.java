package org.texttechnologylab.duui.dua;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DUAXmiBridgeTest {
    @TempDir
    Path temp;

    @Test
    void storesAndMaterializesUimaViewThroughDua() throws Exception {
        Path archive = temp.resolve("cas.dua");
        DUAXmiBridge bridge = new DUAXmiBridge();
        JCas source = JCasFactory.createJCas();
        source.setDocumentLanguage("en");
        source.setDocumentText("DUA carries CAS as one artifact family.");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-cas")) {
            bridge.addCasPayload(source, writer, "doc-1");
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            JCas restored = bridge.materialize(reader, "doc-1");
            assertEquals(source.getDocumentText(), restored.getDocumentText());
            assertEquals("en", restored.getDocumentLanguage());
            assertEquals(1, reader.manifest().getArtifacts().size());
        }
    }

    @Test
    void casPayloadIntegrationUsesDuaGlobalFsIdsAcrossDocuments() throws Exception {
        Path archive = temp.resolve("global-fs-ids.dua");
        DUAXmiBridge bridge = new DUAXmiBridge();
        JCas first = source("first document");
        JCas second = source("second document");

        try (DUAArchiveWriter writer = DUAArchiveWriter.create(archive, "universe-cas")) {
            bridge.addCasPayload(first, writer, "doc-1");
            bridge.addCasPayload(second, writer, "doc-2");
        }

        try (DUAArchiveReader reader = DUAArchiveReader.open(archive)) {
            Set<Integer> firstIds = xmiIds(reader.artifactPayload("doc-1"));
            Set<Integer> secondIds = xmiIds(reader.artifactPayload("doc-2"));

            assertTrue(firstIds.stream().noneMatch(secondIds::contains));
            assertEquals(first.getDocumentText(), bridge.materialize(reader, "doc-1").getDocumentText());
            assertEquals(second.getDocumentText(), bridge.materialize(reader, "doc-2").getDocumentText());
        }
    }

    private static JCas source(String text) throws Exception {
        JCas source = JCasFactory.createJCas();
        source.setDocumentText(text);
        new Annotation(source, 0, Math.min(5, text.length())).addToIndexes();
        return source;
    }

    private static Set<Integer> xmiIds(byte[] payload) {
        var matcher = Pattern.compile("xmi:id=\"(\\d+)\"")
                .matcher(new String(payload, StandardCharsets.UTF_8));
        Set<Integer> ids = new HashSet<>();
        while (matcher.find()) {
            ids.add(Integer.parseInt(matcher.group(1)));
        }
        return ids;
    }
}

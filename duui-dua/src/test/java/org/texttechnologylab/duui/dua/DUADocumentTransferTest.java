package org.texttechnologylab.duui.dua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.texttechnologylab.duui.dua.archive.DUAArchiveReader;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;
import org.texttechnologylab.duui.dua.transport.DUAFsIdMapEntry;
import org.texttechnologylab.duui.dua.transport.DUAFsIdentityMode;
import org.texttechnologylab.duui.dua.transport.DUAFsIdentityPlanner;
import org.texttechnologylab.duui.dua.transport.DUAFsRemapPlan;
import org.texttechnologylab.duui.dua.transport.DUADocumentTransferEntry;
import org.texttechnologylab.duui.dua.transport.DUADocumentTransferManifest;
import org.texttechnologylab.duui.dua.transport.DUADocumentTransferReader;
import org.texttechnologylab.duui.dua.transport.DUADocumentTransferWriter;

class DUADocumentTransferTest {
    @TempDir
    Path temp;

    @Test
    void singleDocumentTransferRoundTripsAsXmi() throws Exception {
        Path transfer = temp.resolve("single.dua-transfer");
        JCas source = JCasFactory.createJCas();
        source.setDocumentLanguage("en");
        source.setDocumentText("single document transfer");

        try (DUADocumentTransferWriter writer = DUADocumentTransferWriter.create(transfer)) {
            writer.addJCas("doc-1", List.of("corpus-a"), source);
        }

        try (DUADocumentTransferReader reader = DUADocumentTransferReader.open(transfer)) {
            assertEquals(List.of("doc-1"), reader.documentIds());
            assertEquals("corpus-a", reader.manifest().documents().get(0).memberships().get(0).corpusId());
            JCas restored = reader.materializeXmi("doc-1");
            assertEquals(source.getDocumentText(), restored.getDocumentText());
            assertEquals("en", restored.getDocumentLanguage());

            Path xmi = temp.resolve("doc-1.xmi");
            reader.exportBareXmi("doc-1", xmi);
            assertTrue(Files.size(xmi) > 0);
        }
    }

    @Test
    void multipleDocumentTransferImportsIntoDuaArchiveCorpus() throws Exception {
        Path transfer = temp.resolve("batch.dua-transfer");
        JCas first = JCasFactory.createJCas();
        first.setDocumentText("first imported document");
        JCas second = JCasFactory.createJCas();
        second.setDocumentText("second imported document");

        try (DUADocumentTransferWriter writer = DUADocumentTransferWriter.create(transfer)) {
            writer.addJCas("doc-1", List.of("source-corpus"), first);
            writer.addJCas("doc-2", List.of("source-corpus"), second);
        }

        Path archive = temp.resolve("imported.dua");
        try (DUADocumentTransferReader transferReader = DUADocumentTransferReader.open(transfer);
             DUAArchiveWriter archiveWriter = DUAArchiveWriter.create(archive, "target-universe")) {
            transferReader.importXmiDocuments(archiveWriter, "target-corpus", new DUAJsonlGraphCodec());
        }

        DUAXmiBridge bridge = new DUAXmiBridge();
        try (DUAArchiveReader archiveReader = DUAArchiveReader.open(archive)) {
            assertEquals(2, archiveReader.manifest().getArtifacts().size());
            assertEquals(2, archiveReader.manifest().getPartitions().size());
            assertEquals(first.getDocumentText(), bridge.materialize(archiveReader, "doc-1").getDocumentText());
            assertEquals(second.getDocumentText(), bridge.materialize(archiveReader, "doc-2").getDocumentText());
            String partition = new String(archiveReader.resourcePayload("graphs/cas-doc-1/jsonl/graph.jsonl"));
            assertTrue(partition.contains("target-corpus"));
        }
    }

    @Test
    void fsIdentityPlannerRequiresExplicitMappingForNativeRemap() {
        DUADocumentTransferEntry document = new DUADocumentTransferEntry(
                "doc-native",
                null,
                0L,
                "attach",
                null,
                null,
                "source-u:doc-native",
                "target-u:doc-native",
                List.of(new DUAFsIdMapEntry("source-u:doc-native:1", "target-u:doc-native:9001",
                        "uima.tcas.Annotation", "_InitialView")),
                List.of(),
                List.of(),
                List.of(),
                java.util.Map.of());
        DUADocumentTransferManifest manifest = new DUADocumentTransferManifest(
                DUADocumentTransferManifest.SCHEMA,
                "transfer-remap",
                "source-u",
                "target-u",
                0,
                "upsert-documents",
                DUAFsIdentityMode.EXPLICIT_REMAP.wireName(),
                "create-revision",
                List.of(),
                List.of(document));

        DUAFsRemapPlan plan = DUAFsIdentityPlanner.forDocument(manifest, document, "target-u:doc-native");

        assertEquals("target-u:doc-native:9001", plan.targetFor("source-u:doc-native:1"));
    }

    @Test
    void fsIdentityPlannerAllocatesSequentialTargetIds() {
        DUAFsRemapPlan plan = DUAFsIdentityPlanner.allocateSequential(
                "source-u:doc",
                "target-u:doc",
                "_InitialView",
                "uima.tcas.Annotation",
                List.of(1L, 2L, 5L),
                100L);

        assertEquals("target-u:doc:100", plan.targetFor("source-u:doc:1"));
        assertEquals("target-u:doc:101", plan.targetFor("source-u:doc:2"));
        assertEquals("target-u:doc:102", plan.targetFor("source-u:doc:5"));
    }
}

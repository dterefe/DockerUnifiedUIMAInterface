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
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.jsonl.DUAJsonlGraphCodec;
import org.texttechnologylab.duui.dua.transport.DUAJCasTransferDocument;
import org.texttechnologylab.duui.dua.transport.DUALocalTransportService;
import org.texttechnologylab.duui.dua.transport.DUAMembershipPatchDocument;
import org.texttechnologylab.duui.dua.transport.DUADocumentTransferReader;
import org.texttechnologylab.duui.dua.transport.DUATransportJob;
import org.texttechnologylab.duui.dua.transport.DUATransportJobStatus;

class DUATransportServiceTest {
    @TempDir
    Path temp;

    @Test
    void serviceExportsTransferImportsArchiveAndReportsJobStatus() throws Exception {
        DUALocalTransportService service = new DUALocalTransportService();
        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText("transport service document");

        Path transfer = temp.resolve("service.dua-transfer");
        DUATransportJob export = service.exportTransfer(transfer,
                List.of(new DUAJCasTransferDocument("doc-service", List.of("corpus-service"), jCas)));

        assertEquals(DUATransportJobStatus.SUCCEEDED, export.status());
        assertEquals(export, service.job(export.jobId()));
        assertTrue(Files.size(transfer) > 0);

        Path archive = temp.resolve("service.dua");
        DUATransportJob importJob = service.importTransferToArchive(
                transfer, archive, "universe-service", "corpus-target", new DUAJsonlGraphCodec());

        assertEquals(DUATransportJobStatus.SUCCEEDED, importJob.status());
        assertEquals(1, importJob.documentCount());
        try (DUAArchiveReader archiveReader = DUAArchiveReader.open(archive)) {
            assertEquals("transport service document",
                    new DUAXmiBridge().materialize(archiveReader, "doc-service").getDocumentText());
        }

        Path xmi = temp.resolve("service.xmi");
        DUATransportJob xmiJob = service.exportBareXmi(transfer, "doc-service", xmi);
        assertEquals(DUATransportJobStatus.SUCCEEDED, xmiJob.status());
        assertTrue(Files.size(xmi) > 0);
    }

    @Test
    void serviceCreatesMembershipPatchTransferWithoutCasPayload() throws Exception {
        DUALocalTransportService service = new DUALocalTransportService();
        Path transfer = temp.resolve("membership.dua-transfer");

        DUATransportJob job = service.createMembershipPatch(transfer,
                List.of(new DUAMembershipPatchDocument("doc-1", List.of("corpus-a", "corpus-b"), "attach")));

        assertEquals(DUATransportJobStatus.SUCCEEDED, job.status());
        try (DUADocumentTransferReader reader = DUADocumentTransferReader.open(transfer)) {
            assertEquals("patch-corpus-membership", reader.manifest().operation());
            assertEquals("xmi-local", reader.manifest().fsIdentityMode());
            assertEquals("doc-1", reader.manifest().documents().get(0).documentId());
            assertEquals(2, reader.manifest().documents().get(0).memberships().size());
            assertEquals(0, reader.manifest().documents().get(0).views().size());
        }
    }
}

package org.texttechnologylab.duui.dua.transport;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;

public final class DUALocalTransportService implements DUATransportService {
    private final Map<String, DUATransportJob> jobs = new ConcurrentHashMap<>();

    @Override
    public DUATransportJob exportTransfer(Path target, List<DUAJCasTransferDocument> documents) {
        List<DUAJCasTransferDocument> safeDocuments = documents == null ? List.of() : List.copyOf(documents);
        return run("export-transfer", null, target, safeDocuments.size(), () -> {
            try (DUADocumentTransferWriter writer = DUADocumentTransferWriter.create(target)) {
                for (DUAJCasTransferDocument document : safeDocuments) {
                    writer.addJCas(document.documentId(), document.corpusIds(), document.jCas());
                }
            }
            return safeDocuments.size();
        });
    }

    @Override
    public DUATransportJob createMembershipPatch(Path target, List<DUAMembershipPatchDocument> documents) {
        List<DUAMembershipPatchDocument> safeDocuments = documents == null ? List.of() : List.copyOf(documents);
        return run("patch-corpus-membership", null, target, safeDocuments.size(), () -> {
            try (DUADocumentTransferWriter writer = DUADocumentTransferWriter.create(
                    target, null, null, null, "patch-corpus-membership", "create-revision")) {
                for (DUAMembershipPatchDocument document : safeDocuments) {
                    writer.addMembershipPatch(document.documentId(), document.corpusIds(), document.operation());
                }
            }
            return safeDocuments.size();
        });
    }

    @Override
    public DUATransportJob importTransferToArchive(Path transfer,
                                                   Path targetArchive,
                                                   String universeId,
                                                   String corpusId,
                                                   DUAGraphCodec codec) {
        return run("import-transfer", transfer, targetArchive, 0, () -> {
            try (DUADocumentTransferReader reader = DUADocumentTransferReader.open(transfer);
                 DUAArchiveWriter writer = DUAArchiveWriter.create(targetArchive, universeId)) {
                reader.importXmiDocuments(writer, corpusId, codec);
                return reader.documentIds().size();
            }
        });
    }

    @Override
    public DUATransportJob exportBareXmi(Path transfer, String documentId, Path target) {
        return run("export-xmi", transfer, target, 1, () -> {
            try (DUADocumentTransferReader reader = DUADocumentTransferReader.open(transfer)) {
                reader.exportBareXmi(documentId, target);
            }
            return 1;
        });
    }

    @Override
    public DUATransportJob importBareXmiToArchive(Path xmi,
                                                  Path targetArchive,
                                                  String universeId,
                                                  String corpusId,
                                                  String documentId,
                                                  DUAGraphCodec codec) {
        return run("import-xmi", xmi, targetArchive, 1, () -> {
            try (DUAArchiveWriter writer = DUAArchiveWriter.create(targetArchive, universeId)) {
                new DUAXmiBridge().importXmi(xmi, writer, corpusId, codec);
            }
            return 1;
        });
    }

    @Override
    public DUATransportJob job(String jobId) {
        DUATransportJob job = jobs.get(jobId);
        if (job == null) {
            throw new DUADocumentTransferException("Unknown transport job: " + jobId);
        }
        return job;
    }

    private DUATransportJob run(String operation,
                                Path source,
                                Path target,
                                int expectedDocumentCount,
                                TransportAction action) {
        String jobId = DUAId.create().value();
        DUATransportJob accepted = new DUATransportJob(
                jobId,
                DUATransportJobStatus.ACCEPTED,
                operation,
                expectedDocumentCount,
                source == null ? null : source.toString(),
                target == null ? null : target.toString(),
                "accepted",
                System.currentTimeMillis(),
                0);
        jobs.put(jobId, accepted.running());
        try {
            int count = action.run();
            DUATransportJob finished = accepted.succeeded(count, "ok");
            jobs.put(jobId, finished);
            return finished;
        } catch (Exception e) {
            DUATransportJob failed = accepted.failed(e.getMessage());
            jobs.put(jobId, failed);
            throw new DUADocumentTransferException("Transport job failed: " + operation, e);
        }
    }

    @FunctionalInterface
    private interface TransportAction {
        int run() throws Exception;
    }
}

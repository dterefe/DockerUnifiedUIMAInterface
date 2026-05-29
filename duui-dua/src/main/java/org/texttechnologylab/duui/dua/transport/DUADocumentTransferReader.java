package org.texttechnologylab.duui.dua.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipFile;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.dua.archive.DUAArchiveWriter;
import org.texttechnologylab.duui.dua.cas.DUAXmiBridge;
import org.texttechnologylab.duui.dua.graph.DUAGraphCodec;

public final class DUADocumentTransferReader implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path transfer;
    private final Path staging;
    private final DUADocumentTransferManifest manifest;

    private DUADocumentTransferReader(Path transfer) throws IOException {
        this.transfer = Objects.requireNonNull(transfer, "transfer");
        this.staging = Files.createTempDirectory("dua-transfer-reader-");
        unzip();
        this.manifest = MAPPER.readValue(
                staging.resolve(DUADocumentTransferWriter.MANIFEST).toFile(),
                DUADocumentTransferManifest.class);
        verifyPayloads();
    }

    public static DUADocumentTransferReader open(Path transfer) throws IOException {
        return new DUADocumentTransferReader(transfer);
    }

    public DUADocumentTransferManifest manifest() {
        return manifest;
    }

    public List<String> documentIds() {
        return manifest.documents().stream().map(DUADocumentTransferEntry::documentId).toList();
    }

    public JCas materializeXmi(String documentId) throws Exception {
        DUADocumentTransferEntry document = document(documentId);
        DUAViewTransfer view = firstXmiView(document)
                .orElseThrow(() -> new DUADocumentTransferException("No XMI view for document " + documentId));
        JCas jCas = JCasFactory.createJCas();
        try (var input = new ByteArrayInputStream(Files.readAllBytes(staging.resolve(view.payload().path())))) {
            CasIOUtils.load(input, jCas.getCas());
        }
        return jCas;
    }

    public void exportBareXmi(String documentId, Path target) throws IOException {
        DUADocumentTransferEntry document = document(documentId);
        DUAViewTransfer view = firstXmiView(document)
                .orElseThrow(() -> new DUADocumentTransferException("No XMI view for document " + documentId));
        Path parent = target.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.copy(staging.resolve(view.payload().path()), target);
    }

    public void importXmiDocuments(DUAArchiveWriter writer, String corpusId, DUAGraphCodec codec) throws Exception {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(codec, "codec");
        DUAXmiBridge bridge = new DUAXmiBridge();
        for (DUADocumentTransferEntry document : manifest.documents()) {
            if (document.operation().equals("delete") || firstXmiView(document).isEmpty()) {
                continue;
            }
            String effectiveCorpusId = corpusId == null || corpusId.isBlank()
                    ? firstAttachedCorpus(document).orElse("imported")
                    : corpusId;
            bridge.addJCas(materializeXmi(document.documentId()), writer, effectiveCorpusId, document.documentId(), codec);
        }
    }

    @Override
    public void close() throws IOException {
        try (var paths = Files.walk(staging)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private DUADocumentTransferEntry document(String documentId) {
        return manifest.documents().stream()
                .filter(document -> document.documentId().equals(documentId))
                .findFirst()
                .orElseThrow(() -> new DUADocumentTransferException("No document in transfer: " + documentId));
    }

    private Optional<DUAViewTransfer> firstXmiView(DUADocumentTransferEntry document) {
        return document.views().stream().filter(view -> view.encoding().equals("xmi-1.1")).findFirst();
    }

    private Optional<String> firstAttachedCorpus(DUADocumentTransferEntry document) {
        return document.memberships().stream()
                .filter(membership -> membership.operation().equals("attach"))
                .map(DUACorpusMembershipTransfer::corpusId)
                .findFirst();
    }

    private void verifyPayloads() throws IOException {
        for (DUADocumentTransferEntry document : manifest.documents()) {
            for (DUAViewTransfer view : document.views()) {
                verify(view.payload());
            }
            for (DUATransferObjectRef payload : document.payloads()) {
                verify(payload);
            }
        }
        for (DUATransferObjectRef typeSystem : manifest.typeSystems()) {
            verify(typeSystem);
        }
    }

    private void verify(DUATransferObjectRef ref) throws IOException {
        Path payload = staging.resolve(ref.path()).normalize();
        if (!payload.startsWith(staging)) {
            throw new DUADocumentTransferException("Illegal transfer payload path: " + ref.path());
        }
        if (!Files.exists(payload)) {
            throw new DUADocumentTransferException("Missing transfer payload: " + ref.path());
        }
        long size = Files.size(payload);
        if (size != ref.byteLength()) {
            throw new DUADocumentTransferException("Payload size mismatch for " + ref.path());
        }
        String actual = DUADocumentTransferWriter.sha256(payload);
        if (!actual.equalsIgnoreCase(ref.sha256())) {
            throw new DUADocumentTransferException("Payload checksum mismatch for " + ref.path());
        }
    }

    private void unzip() throws IOException {
        try (ZipFile zip = new ZipFile(transfer.toFile())) {
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                var entry = entries.nextElement();
                Path target = staging.resolve(entry.getName()).normalize();
                if (!target.startsWith(staging)) {
                    throw new DUADocumentTransferException("Illegal transfer ZIP entry path: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    try (var input = zip.getInputStream(entry)) {
                        Files.copy(input, target);
                    }
                }
            }
        }
    }
}

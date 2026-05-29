package org.texttechnologylab.duui.dua.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.duui.dua.DUAId;

public final class DUADocumentTransferWriter implements Closeable {
    public static final String MANIFEST = "duat.json";
    public static final String MEDIA_TYPE_XMI = "application/vnd.apache.uima.cas+xmi";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Path output;
    private final Path staging;
    private final String transferId;
    private final String sourceUniverseId;
    private final String targetUniverseId;
    private final String operation;
    private final String mergePolicy;
    private final List<DUADocumentTransferEntry> documents = new ArrayList<>();
    private boolean closed;

    private DUADocumentTransferWriter(Path output,
                                      String transferId,
                                      String sourceUniverseId,
                                      String targetUniverseId,
                                      String operation,
                                      String mergePolicy) throws IOException {
        this.output = Objects.requireNonNull(output, "output");
        this.transferId = transferId == null || transferId.isBlank() ? DUAId.create().value() : transferId;
        this.sourceUniverseId = sourceUniverseId;
        this.targetUniverseId = targetUniverseId;
        this.operation = operation == null || operation.isBlank() ? "upsert-documents" : operation;
        this.mergePolicy = mergePolicy == null || mergePolicy.isBlank() ? "create-revision" : mergePolicy;
        this.staging = Files.createTempDirectory("dua-transfer-writer-");
    }

    public static DUADocumentTransferWriter create(Path output) throws IOException {
        return new DUADocumentTransferWriter(output, null, null, null, "upsert-documents", "create-revision");
    }

    public static DUADocumentTransferWriter create(Path output,
                                                   String transferId,
                                                   String sourceUniverseId,
                                                   String targetUniverseId,
                                                   String operation,
                                                   String mergePolicy) throws IOException {
        return new DUADocumentTransferWriter(output, transferId, sourceUniverseId, targetUniverseId,
                operation, mergePolicy);
    }

    public DUADocumentTransferWriter addJCas(String documentId,
                                             List<String> corpusIds,
                                             JCas jCas) throws IOException {
        return addXmiDocument(documentId, corpusIds, serialize(jCas), Map.of());
    }

    public DUADocumentTransferWriter addXmiFile(String documentId,
                                                List<String> corpusIds,
                                                Path xmi) throws IOException {
        return addXmiDocument(documentId, corpusIds, Files.readAllBytes(xmi), Map.of());
    }

    public DUADocumentTransferWriter addXmiDocument(String documentId,
                                                    List<String> corpusIds,
                                                    byte[] xmi,
                                                    Map<String, Object> metadata) throws IOException {
        Objects.requireNonNull(xmi, "xmi");
        String effectiveDocumentId = documentId == null || documentId.isBlank() ? DUAId.create().value() : documentId;
        String viewName = "_InitialView";
        String payloadPath = "documents/" + sanitize(effectiveDocumentId) + "/views/" + viewName + ".xmi";
        Path target = staging.resolve(payloadPath);
        Files.createDirectories(target.getParent());
        Files.write(target, xmi);

        DUATransferObjectRef ref = new DUATransferObjectRef(
                effectiveDocumentId + ":" + viewName,
                payloadPath,
                MEDIA_TYPE_XMI,
                sha256(target),
                xmi.length);
        List<DUACorpusMembershipTransfer> memberships = corpusIds == null ? List.of() : corpusIds.stream()
                .map(corpusId -> new DUACorpusMembershipTransfer(corpusId, "attach", null))
                .toList();
        documents.add(new DUADocumentTransferEntry(
                effectiveDocumentId,
                null,
                0L,
                "upsert",
                ref.sha256(),
                null,
                null,
                null,
                List.of(),
                memberships,
                List.of(new DUAViewTransfer(viewName, "text/plain", "xmi-1.1", ref)),
                List.of(),
                metadata));
        return this;
    }

    public DUADocumentTransferWriter addMembershipPatch(String documentId,
                                                        List<String> corpusIds,
                                                        String membershipOperation) {
        String effectiveDocumentId = documentId == null || documentId.isBlank() ? DUAId.create().value() : documentId;
        String effectiveOperation = membershipOperation == null || membershipOperation.isBlank()
                ? "attach"
                : membershipOperation;
        List<DUACorpusMembershipTransfer> memberships = corpusIds == null ? List.of() : corpusIds.stream()
                .map(corpusId -> new DUACorpusMembershipTransfer(corpusId, effectiveOperation, null))
                .toList();
        documents.add(new DUADocumentTransferEntry(
                effectiveDocumentId,
                null,
                0L,
                effectiveOperation,
                null,
                null,
                null,
                null,
                List.of(),
                memberships,
                List.of(),
                List.of(),
                Map.of()));
        return this;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        DUADocumentTransferManifest manifest = new DUADocumentTransferManifest(
                transferId,
                sourceUniverseId,
                targetUniverseId,
                operation,
                mergePolicy,
                List.of(),
                documents);
        MAPPER.writeValue(staging.resolve(MANIFEST).toFile(), manifest);
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (OutputStream file = Files.newOutputStream(output);
             ZipOutputStream zip = new ZipOutputStream(file)) {
            try (var paths = Files.walk(staging)) {
                for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                    String entryName = staging.relativize(path).toString().replace('\\', '/');
                    zip.putNextEntry(new ZipEntry(entryName));
                    Files.copy(path, zip);
                    zip.closeEntry();
                }
            }
        } finally {
            deleteStaging();
        }
    }

    private void deleteStaging() throws IOException {
        if (!Files.exists(staging)) {
            return;
        }
        try (var paths = Files.walk(staging)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    static byte[] serialize(JCas jCas) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            CasIOUtils.save(jCas.getCas(), output, SerialFormat.XMI_1_1);
            return output.toByteArray();
        }
    }

    static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            StringBuilder builder = new StringBuilder(64);
            for (byte b : digest.digest()) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}

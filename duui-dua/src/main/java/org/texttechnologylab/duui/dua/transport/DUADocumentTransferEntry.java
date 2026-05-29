package org.texttechnologylab.duui.dua.transport;

import java.util.List;
import java.util.Map;

public record DUADocumentTransferEntry(String documentId,
                                       String sourceDocumentId,
                                       Long revision,
                                       String operation,
                                       String contentSha256,
                                       String typeSystemId,
                                       String sourceFsNamespace,
                                       String targetFsNamespace,
                                       List<DUAFsIdMapEntry> fsIdMap,
                                       List<DUACorpusMembershipTransfer> memberships,
                                       List<DUAViewTransfer> views,
                                       List<DUATransferObjectRef> payloads,
                                       Map<String, Object> metadata) {
    public DUADocumentTransferEntry {
        if (documentId == null || documentId.isBlank()) {
            throw new IllegalArgumentException("documentId must not be blank");
        }
        if (revision != null && revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
        operation = operation == null || operation.isBlank() ? "upsert" : operation;
        if (!operation.equals("upsert")
                && !operation.equals("delete")
                && !operation.equals("attach")
                && !operation.equals("detach")) {
            throw new IllegalArgumentException("Unsupported document operation: " + operation);
        }
        if (contentSha256 != null && !contentSha256.matches("^[a-fA-F0-9]{64}$")) {
            throw new IllegalArgumentException("contentSha256 must be a 64 character hex string");
        }
        fsIdMap = fsIdMap == null ? List.of() : List.copyOf(fsIdMap);
        memberships = memberships == null ? List.of() : List.copyOf(memberships);
        views = views == null ? List.of() : List.copyOf(views);
        payloads = payloads == null ? List.of() : List.copyOf(payloads);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        if (operation.equals("upsert") && views.isEmpty()) {
            throw new IllegalArgumentException("upsert document transfers require at least one view");
        }
    }
}

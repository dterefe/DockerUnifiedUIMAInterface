package org.texttechnologylab.duui.dua.transport;

import java.time.Instant;
import java.util.List;

public record DUADocumentTransferManifest(String schema,
                                          String transferId,
                                          String sourceUniverseId,
                                          String targetUniverseId,
                                          long createdEpochMs,
                                          String operation,
                                          String fsIdentityMode,
                                          String mergePolicy,
                                          List<DUATransferObjectRef> typeSystems,
                                          List<DUADocumentTransferEntry> documents) {
    public static final String SCHEMA = "dua.transport.document-transfer.v1";

    public DUADocumentTransferManifest(String transferId,
                                       String sourceUniverseId,
                                       String targetUniverseId,
                                       String operation,
                                       String mergePolicy,
                                       List<DUATransferObjectRef> typeSystems,
                                       List<DUADocumentTransferEntry> documents) {
        this(SCHEMA, transferId, sourceUniverseId, targetUniverseId, Instant.now().toEpochMilli(),
                operation, DUAFsIdentityMode.XMI_LOCAL.wireName(), mergePolicy, typeSystems, documents);
    }

    public DUADocumentTransferManifest {
        if (!SCHEMA.equals(schema)) {
            throw new IllegalArgumentException("Unsupported document transfer schema: " + schema);
        }
        if (transferId == null || transferId.isBlank()) {
            throw new IllegalArgumentException("transferId must not be blank");
        }
        if (createdEpochMs < 0) {
            throw new IllegalArgumentException("createdEpochMs must not be negative");
        }
        operation = operation == null || operation.isBlank() ? "upsert-documents" : operation;
        if (!operation.equals("export-snapshot")
                && !operation.equals("upsert-documents")
                && !operation.equals("patch-corpus-membership")
                && !operation.equals("delete-documents")) {
            throw new IllegalArgumentException("Unsupported transfer operation: " + operation);
        }
        fsIdentityMode = fsIdentityMode == null || fsIdentityMode.isBlank()
                ? DUAFsIdentityMode.XMI_LOCAL.wireName()
                : fsIdentityMode;
        DUAFsIdentityMode.fromWireName(fsIdentityMode);
        mergePolicy = mergePolicy == null || mergePolicy.isBlank() ? "create-revision" : mergePolicy;
        if (!mergePolicy.equals("fail-on-conflict")
                && !mergePolicy.equals("create-revision")
                && !mergePolicy.equals("replace-document")
                && !mergePolicy.equals("keep-existing")) {
            throw new IllegalArgumentException("Unsupported merge policy: " + mergePolicy);
        }
        typeSystems = typeSystems == null ? List.of() : List.copyOf(typeSystems);
        documents = documents == null ? List.of() : List.copyOf(documents);
        if (documents.isEmpty()) {
            throw new IllegalArgumentException("documents must not be empty");
        }
    }
}

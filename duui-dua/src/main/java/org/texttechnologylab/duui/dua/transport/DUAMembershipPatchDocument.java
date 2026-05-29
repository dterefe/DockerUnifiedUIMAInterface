package org.texttechnologylab.duui.dua.transport;

import java.util.List;

public record DUAMembershipPatchDocument(String documentId, List<String> corpusIds, String operation) {
    public DUAMembershipPatchDocument {
        if (documentId == null || documentId.isBlank()) {
            throw new IllegalArgumentException("documentId must not be blank");
        }
        corpusIds = corpusIds == null ? List.of() : List.copyOf(corpusIds);
        operation = operation == null || operation.isBlank() ? "attach" : operation;
        if (!operation.equals("attach") && !operation.equals("detach")) {
            throw new IllegalArgumentException("Unsupported membership operation: " + operation);
        }
    }
}

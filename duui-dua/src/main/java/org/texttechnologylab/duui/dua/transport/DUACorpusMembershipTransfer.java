package org.texttechnologylab.duui.dua.transport;

public record DUACorpusMembershipTransfer(String corpusId,
                                          String operation,
                                          Long membershipRevision) {
    public DUACorpusMembershipTransfer {
        if (corpusId == null || corpusId.isBlank()) {
            throw new IllegalArgumentException("corpusId must not be blank");
        }
        operation = operation == null || operation.isBlank() ? "attach" : operation;
        if (!operation.equals("attach") && !operation.equals("detach")) {
            throw new IllegalArgumentException("Unsupported corpus membership operation: " + operation);
        }
        if (membershipRevision != null && membershipRevision < 0) {
            throw new IllegalArgumentException("membershipRevision must not be negative");
        }
    }
}

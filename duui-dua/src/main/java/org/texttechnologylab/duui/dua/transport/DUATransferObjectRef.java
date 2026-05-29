package org.texttechnologylab.duui.dua.transport;

public record DUATransferObjectRef(String id,
                                   String path,
                                   String mediaType,
                                   String sha256,
                                   long byteLength) {
    public DUATransferObjectRef {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        if (mediaType == null || mediaType.isBlank()) {
            throw new IllegalArgumentException("mediaType must not be blank");
        }
        if (sha256 == null || !sha256.matches("^[a-fA-F0-9]{64}$")) {
            throw new IllegalArgumentException("sha256 must be a 64 character hex string");
        }
        if (byteLength < 0) {
            throw new IllegalArgumentException("byteLength must not be negative");
        }
    }
}

package org.texttechnologylab.duui.dua.distributed;

public record DUAShardObjectRef(String uri, String sha256, long byteLength) {
    public DUAShardObjectRef {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri must not be blank");
        }
        if (sha256 == null || !sha256.matches("^[a-fA-F0-9]{64}$")) {
            throw new IllegalArgumentException("sha256 must be a 64 character hex string");
        }
        if (byteLength < 0) {
            throw new IllegalArgumentException("byteLength must not be negative");
        }
    }
}

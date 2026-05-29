package org.texttechnologylab.duui.dua.transport;

public record DUAViewTransfer(String viewName,
                              String sofaMimeType,
                              String encoding,
                              DUATransferObjectRef payload) {
    public DUAViewTransfer {
        if (viewName == null || viewName.isBlank()) {
            throw new IllegalArgumentException("viewName must not be blank");
        }
        encoding = encoding == null || encoding.isBlank() ? "xmi-1.1" : encoding;
        if (!encoding.equals("xmi-1.1")
                && !encoding.equals("dua-kv-wal-v1")
                && !encoding.equals("dua-kv-snapshot-v1")
                && !encoding.equals("text")
                && !encoding.equals("binary-payload")) {
            throw new IllegalArgumentException("Unsupported view encoding: " + encoding);
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
    }
}

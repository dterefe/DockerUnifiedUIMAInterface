package org.texttechnologylab.duui.dua.transport;

public record DUAFsIdMapEntry(String sourceFsId, String targetFsId, String typeName, String viewName) {
    public DUAFsIdMapEntry {
        if (sourceFsId == null || sourceFsId.isBlank()) {
            throw new IllegalArgumentException("sourceFsId must not be blank");
        }
        if (targetFsId == null || targetFsId.isBlank()) {
            throw new IllegalArgumentException("targetFsId must not be blank");
        }
    }
}

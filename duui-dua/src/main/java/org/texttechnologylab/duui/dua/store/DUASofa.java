package org.texttechnologylab.duui.dua.store;

public record DUASofa(long fsRef, String sofaId, String localName, byte[] data, SofaType type, long createdEpochMs) {
    public enum SofaType { TEXT, BYTES, URI }

    public String text() {
        if (type == SofaType.TEXT && data != null) {
            return new String(data, java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Not a text sofa: " + type);
    }

    public static DUASofa ofText(long fsRef, String sofaId, String localName, String text) {
        return new DUASofa(fsRef, sofaId, localName, text.getBytes(java.nio.charset.StandardCharsets.UTF_8), SofaType.TEXT, System.currentTimeMillis());
    }
}

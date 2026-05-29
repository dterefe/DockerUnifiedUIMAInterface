package org.texttechnologylab.duui.runtime;

public final class DUUI {
    private DUUI() {
    }

    public static DUUISystemScope system(String id) {
        return new DUUISystemScope(id);
    }
}

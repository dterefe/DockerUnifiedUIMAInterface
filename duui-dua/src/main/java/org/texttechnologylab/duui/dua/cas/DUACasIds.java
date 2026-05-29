package org.texttechnologylab.duui.dua.cas;

import org.texttechnologylab.duui.dua.DUAId;

final class DUACasIds {
    private DUACasIds() {
    }

    static String create(String prefix) {
        return prefix + ":" + DUAId.create().value();
    }
}

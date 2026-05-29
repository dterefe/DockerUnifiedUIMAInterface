package org.texttechnologylab.duui.dua.transport;

public enum DUAFsIdentityMode {
    XMI_LOCAL("xmi-local"),
    STABLE_GLOBAL_GID("stable-global-gid"),
    EXPLICIT_REMAP("explicit-remap");

    private final String wireName;

    DUAFsIdentityMode(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static DUAFsIdentityMode fromWireName(String wireName) {
        for (DUAFsIdentityMode mode : values()) {
            if (mode.wireName.equals(wireName)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unsupported FS identity mode: " + wireName);
    }
}

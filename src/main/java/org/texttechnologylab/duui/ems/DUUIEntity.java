package org.texttechnologylab.duui.ems;

public interface DUUIEntity {
    default GID gid() {
        return GID.of(getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this)));
    }

    default DUUITraits traits() {
        return DUUITraits.empty();
    }

    default String id() {
        return gid().toString();
    }
}

package org.texttechnologylab.duui.ems;

public interface DUUIEntity {
    default GID gid() {
        return DUUIEntityIds.gid(this);
    }

    default DUUITraits traits() {
        return DUUITraits.empty();
    }

    default String id() {
        return gid().toString();
    }
}

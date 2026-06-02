package org.texttechnologylab.duui.ems;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

final class DUUIEntityIds {
    private static final Map<DUUIEntity, GID> IDS = Collections.synchronizedMap(new WeakHashMap<>());

    private DUUIEntityIds() {
    }

    static GID gid(DUUIEntity entity) {
        synchronized (IDS) {
            return IDS.computeIfAbsent(entity, value -> GID.create(value.getClass()));
        }
    }
}

package org.texttechnologylab.duui.ems;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DUUIActor extends DUUIEntity {
    private final ConcurrentHashMap<String, Object> instructions = new ConcurrentHashMap<>();

    protected DUUIActor(String id) {
        super(id);
    }

    public Map<String, Object> instructions() {
        return Map.copyOf(instructions);
    }

    public void instruction(String key, Object value) {
        if (value == null) {
            instructions.remove(key);
            return;
        }
        instructions.put(key, value);
    }
}

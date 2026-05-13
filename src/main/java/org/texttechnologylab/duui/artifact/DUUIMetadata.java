package org.texttechnologylab.duui.artifact;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DUUIMetadata {
    private final Map<String, String> values = new LinkedHashMap<>();

    public void put(String key, String value) {
        if (key != null && value != null) values.put(key, value);
    }

    public void remove(String key) {
        if (key != null) values.remove(key);
    }

    public String get(String key) { return values.get(key); }
    public Map<String, String> asMap() { return Collections.unmodifiableMap(values); }
}

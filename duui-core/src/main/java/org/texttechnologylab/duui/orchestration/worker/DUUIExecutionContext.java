package org.texttechnologylab.duui.orchestration.worker;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.DUUIFrameworkStateException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DUUIExecutionContext {
    private final List<DUUIArtifact<?>> emittedArtifacts = new ArrayList<>();
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    public void emit(DUUIArtifact<?> artifact) {
        if (artifact != null) emittedArtifacts.add(artifact);
    }

    public List<DUUIArtifact<?>> drainEmittedArtifacts() {
        List<DUUIArtifact<?>> drained = new ArrayList<>(emittedArtifacts);
        emittedArtifacts.clear();
        return Collections.unmodifiableList(drained);
    }

    public void put(String key, Object value) {
        if (value == null) {
            values.remove(key);
            return;
        }
        values.put(key, value);
    }

    public <T> void put(Class<T> key, T value) {
        put(key.getName(), value);
    }

    public Object get(String key) {
        return values.get(key);
    }

    public <T> T get(Class<T> key) {
        return key.cast(values.get(key.getName()));
    }

    public <T> T require(Class<T> key) {
        T value = get(key);
        if (value == null) {
            throw new DUUIFrameworkStateException("Missing execution context value for " + key.getName());
        }
        return value;
    }

    public DUUIEventContext eventContext() {
        return get(DUUIEventContext.class);
    }

    public DUUIExecutionContext eventContext(DUUIEventContext context) {
        put(DUUIEventContext.class, context);
        return this;
    }

    public DUUIEventService eventService() {
        return get(DUUIEventService.class);
    }

    public DUUIExecutionContext eventService(DUUIEventService service) {
        put(DUUIEventService.class, service);
        return this;
    }

    public DUUIExecutionContext copyValues() {
        DUUIExecutionContext copy = new DUUIExecutionContext();
        copy.values.putAll(values);
        return copy;
    }
}

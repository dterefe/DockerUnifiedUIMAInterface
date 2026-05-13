package org.texttechnologylab.duui.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DUUIInMemoryEventSink implements DUUIEventSink {
    private final CopyOnWriteArrayList<DUUIEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void accept(DUUIEvent event) {
        events.add(event);
    }

    public List<DUUIEvent> events() {
        return new ArrayList<>(events);
    }

    public void clear() {
        events.clear();
    }
}

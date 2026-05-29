package org.texttechnologylab.duui.event;

public interface DUUIEventSink extends AutoCloseable {
    void accept(DUUIEvent event);

    @Override
    default void close() {
    }
}

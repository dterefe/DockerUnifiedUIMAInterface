package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

/**
 * Observer that can receive {@link DUUIEvent} instances emitted by
 * the {@link org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer}.
 */
public interface DUUIEventObserver {

    /**
     * Called asynchronously whenever a new {@link DUUIEvent} is added.
     * Implementations should be fast and non-blocking where possible.
     *
     * @param event the event that was emitted
     */
    void onEvent(DUUIEvent event);
}


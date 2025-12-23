package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;

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
    void onEvent(DUUIEvent event, DUUIState state);
    
    void onEvent(DUUIEvent event, DUUIDocument document);
}

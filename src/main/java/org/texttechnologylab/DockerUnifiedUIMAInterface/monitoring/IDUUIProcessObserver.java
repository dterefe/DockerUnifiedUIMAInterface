package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.util.List;

/**
 * Observer for materialized process updates derived from {@link DUUIEvent}.
 * <p>
 * Called by {@link DUUIProcess} in event order.
 */
public interface IDUUIProcessObserver {
    void onEvent(DUUIEvent event, List<DUUIProcess.Update> update);
}


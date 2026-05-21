package org.texttechnologylab.duui.refactor;

public interface DUUIClient<P extends DUUIProxy> {
    P proxy(DUUIAddress address);

    void shutdown();
}

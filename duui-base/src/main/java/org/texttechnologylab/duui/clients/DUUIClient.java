package org.texttechnologylab.duui.clients;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.ems.DUUIService;

public interface DUUIClient<P extends DUUIProxy> extends DUUIService {
    P proxy(DUUIAddress address);

    void shutdown();
}

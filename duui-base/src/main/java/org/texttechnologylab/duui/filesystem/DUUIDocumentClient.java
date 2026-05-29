package org.texttechnologylab.duui.filesystem;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.DUUIClient;

public interface DUUIDocumentClient extends DUUIClient<DUUIFileSystemObject> {
    DUUIFile file(DUUIAddress address);

    DUUIDirectory directory(DUUIAddress address);

    DUUIExplorer explorer(DUUIDirectory directory);

    default DUUIExplorer explorer(DUUIAddress address) {
        return explorer(directory(address));
    }
}

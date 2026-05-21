package org.texttechnologylab.duui.refactor.filesystem;

import org.texttechnologylab.duui.refactor.DUUIAddress;
import org.texttechnologylab.duui.refactor.DUUIClient;

public interface DUUIDocumentClient extends DUUIClient<DUUIFileSystemObject> {
    DUUIFile file(DUUIAddress address);

    DUUIDirectory directory(DUUIAddress address);

    DUUIExplorer explorer(DUUIDirectory directory);

    default DUUIExplorer explorer(DUUIAddress address) {
        return explorer(directory(address));
    }
}

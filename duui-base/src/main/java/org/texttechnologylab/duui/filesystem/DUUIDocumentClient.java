package org.texttechnologylab.duui.filesystem;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.DUUIClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

public interface DUUIDocumentClient extends DUUIClient<DUUIFileSystemObject> {
    DUUIFile file(DUUIAddress address);

    DUUIDirectory directory(DUUIAddress address);

    DUUIExplorer explorer(DUUIDirectory directory);

    default DUUIStream<InputStream> read(DUUIFile file) {
        return file.read();
    }

    default DUUIStream<InputStream> read(DUUIAddress address) {
        return read(file(address));
    }

    DUUIFile write(DUUIAddress address, InputStream input) throws IOException;

    default Stream<DUUIFileSystemObject> list(DUUIDirectory directory) {
        return directory.children();
    }

    default Stream<DUUIFileSystemObject> list(DUUIAddress address) {
        return list(directory(address));
    }

    default DUUIExplorer explorer(DUUIAddress address) {
        return explorer(directory(address));
    }
}

package org.texttechnologylab.duui.refactor.filesystem;

import java.io.InputStream;

public interface DUUIFile extends DUUIFileSystemObject {
    DUUIStream<InputStream> read();

    default String extension() {
        return metadata().extension();
    }

    default String mediaType() {
        return metadata().mediaType();
    }
}

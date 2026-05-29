package org.texttechnologylab.duui.filesystem;

import org.texttechnologylab.duui.clients.handle.DUUIProxy;

public interface DUUIFileSystemObject extends DUUIProxy {
    DUUIFileMetadata metadata();

    String name();

    default boolean exists() {
        return metadata().exists();
    }

    default String path() {
        return metadata().path();
    }

    default long size() {
        return metadata().size();
    }
}

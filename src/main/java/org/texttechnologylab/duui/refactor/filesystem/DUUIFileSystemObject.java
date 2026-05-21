package org.texttechnologylab.duui.refactor.filesystem;

import org.texttechnologylab.duui.refactor.DUUIProxy;

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

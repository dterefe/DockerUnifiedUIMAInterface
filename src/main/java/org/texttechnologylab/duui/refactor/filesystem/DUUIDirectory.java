package org.texttechnologylab.duui.refactor.filesystem;

import java.util.stream.Stream;

public interface DUUIDirectory extends DUUIFileSystemObject {
    DUUIExplorer explorer();

    Stream<DUUIFileSystemObject> children();
}

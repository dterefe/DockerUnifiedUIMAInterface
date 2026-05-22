package org.texttechnologylab.duui.filesystem;

import java.util.stream.Stream;

public interface DUUIDirectory extends DUUIFileSystemObject {
    DUUIExplorer explorer();

    Stream<DUUIFileSystemObject> children();
}

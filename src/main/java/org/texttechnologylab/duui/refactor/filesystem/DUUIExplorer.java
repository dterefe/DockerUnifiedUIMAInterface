package org.texttechnologylab.duui.refactor.filesystem;

import org.texttechnologylab.duui.refactor.DUUIProxy;

import java.util.Map;
import java.util.stream.Stream;

public interface DUUIExplorer extends DUUIProxy {
    DUUIDirectory directory();

    Stream<DUUIFileSystemObject> current();

    Stream<DUUIFileSystemObject> complete();

    Stream<DUUIFileSystemObject> breadthFirst();

    Stream<DUUIFileSystemObject> breadthFirst(int depth);

    Stream<DUUIFileSystemObject> search(String name);

    Stream<DUUIFileSystemObject> search(Map<String, String> attributes);
}

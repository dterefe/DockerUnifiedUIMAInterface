package org.texttechnologylab.duui.refactor.filesystem;

import java.util.stream.Stream;

public interface DUUIStream<T> {
    Stream<T> stream();

    void cancel();

    boolean cancelled();
}

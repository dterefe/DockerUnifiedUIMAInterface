package org.texttechnologylab.duui.filesystem;

import org.texttechnologylab.duui.ems.DUUIResource;

import java.util.stream.Stream;

public interface DUUIStream<T> extends DUUIResource {
    Stream<T> stream();

    void cancel();

    boolean cancelled();
}

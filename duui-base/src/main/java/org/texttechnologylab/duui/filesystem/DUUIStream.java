package org.texttechnologylab.duui.filesystem;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface DUUIStream<T> extends AutoCloseable {
    Stream<T> stream();

    default DUUIStream<T> sink(Consumer<? super T> sink) {
        stream().forEach(sink);
        return this;
    }

    void cancel();

    boolean cancelled();

    default boolean open() {
        return !cancelled();
    }

    @Override
    default void close() {
        cancel();
    }
}

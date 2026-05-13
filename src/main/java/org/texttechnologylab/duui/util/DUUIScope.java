package org.texttechnologylab.duui.util;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class DUUIScope<T> implements AutoCloseable {
    private final T value;
    private final Consumer<T> onClose;
    private boolean closed;

    private DUUIScope(T value, Consumer<T> onClose) {
        this.value = Objects.requireNonNull(value, "value");
        this.onClose = onClose;
    }

    public static <T> DUUIScope<T> of(T value, Consumer<T> onClose) {
        return new DUUIScope<>(value, onClose);
    }

    public static <T> DUUIScope<T> create(Supplier<T> supplier, Consumer<T> onClose) {
        return new DUUIScope<>(supplier.get(), onClose);
    }

    public T value() { return value; }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (onClose != null) onClose.accept(value);
    }
}

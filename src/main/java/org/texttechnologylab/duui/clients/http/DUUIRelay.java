package org.texttechnologylab.duui.clients.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class DUUIRelay<T> implements AutoCloseable {
    private volatile InputStream input;
    private volatile OutputStream output;
    private volatile CompletableFuture<T> future;
    private volatile Consumer<Throwable> cancelHandler = throwable -> { };
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public DUUIRelay() throws IOException {
        reset();
    }

    public InputStream inputStream() {
        return input;
    }

    public OutputStream outputStream() {
        return output;
    }

    public CompletableFuture<T> future() {
        return future;
    }

    public void complete(T value) {
        future.complete(value);
    }

    public void onCancel(Consumer<Throwable> handler) {
        this.cancelHandler = Objects.requireNonNull(handler, "handler");
    }

    public void cancel(Throwable throwable) {
        Throwable cause = throwable == null ? new IOException("DUUI relay cancelled") : throwable;
        if (!failure.compareAndSet(null, cause)) {
            return;
        }
        future.completeExceptionally(cause);
        closeQuietly(output);
        closeQuietly(input);
        cancelHandler.accept(cause);
    }

    public void reset() throws IOException {
        failure.set(null);
        future = new CompletableFuture<>();
        Pipe pipe = Pipe.open();
        input = Channels.newInputStream(pipe.source());
        output = Channels.newOutputStream(pipe.sink());
    }

    @Override
    public void close() {
        closeQuietly(output);
        closeQuietly(input);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}

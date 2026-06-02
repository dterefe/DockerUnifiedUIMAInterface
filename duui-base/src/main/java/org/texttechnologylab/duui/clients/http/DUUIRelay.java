package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.exception.DUUICancellationException;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class DUUIRelay<T> implements AutoCloseable {
    private static final int PIPE_BUFFER_SIZE = 1024 * 1024;

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
        future.completeExceptionally(cancellation(cause));
        closeQuietly(output);
        closeQuietly(input);
        cancelHandler.accept(cause);
    }

    public synchronized void reset() throws IOException {
        failure.set(null);
        future = new CompletableFuture<>();
        PipedInputStream pipeInput = new PipedInputStream(PIPE_BUFFER_SIZE);
        PipedOutputStream pipeOutput = new PipedOutputStream(pipeInput);
        input = new RelayInputStream(pipeInput);
        output = new RelayOutputStream(pipeOutput);
    }

    @Override
    public void close() {
        closeQuietly(output);
        closeQuietly(input);
    }

    private void throwIfCancelled() throws DUUICancellationException {
        Throwable cause = failure.get();
        if (cause != null) {
            throw cancellation(cause);
        }
    }

    private DUUICancellationException cancelFromStream(IOException error) {
        cancel(error);
        return cancellation(error);
    }

    private static DUUICancellationException cancellation(Throwable cause) {
        if (cause instanceof DUUICancellationException cancellation) {
            return cancellation;
        }
        return new DUUICancellationException(cause);
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

    private final class RelayInputStream extends FilterInputStream {
        private RelayInputStream(InputStream input) {
            super(input);
        }

        @Override
        public int read() throws IOException {
            throwIfCancelled();
            try {
                return super.read();
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            throwIfCancelled();
            try {
                return super.read(bytes, offset, length);
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }

        @Override
        public long skip(long length) throws IOException {
            throwIfCancelled();
            try {
                return super.skip(length);
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }
    }

    private final class RelayOutputStream extends FilterOutputStream {
        private RelayOutputStream(OutputStream output) {
            super(output);
        }

        @Override
        public void write(int value) throws IOException {
            throwIfCancelled();
            try {
                super.write(value);
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            throwIfCancelled();
            try {
                out.write(bytes, offset, length);
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }

        @Override
        public void flush() throws IOException {
            throwIfCancelled();
            try {
                super.flush();
            } catch (IOException error) {
                throw cancelFromStream(error);
            }
        }
    }
}

package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.filesystem.DUUIStream;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.net.http.HttpResponse;

public final class DUUIStreamBodyHandler<T> implements HttpResponse.BodyHandler<DUUIStream<T>> {
    private final DUUIDeserializer<T> chunkDeserializer;

    public DUUIStreamBodyHandler(DUUIDeserializer<T> chunkDeserializer) {
        this.chunkDeserializer = Objects.requireNonNull(chunkDeserializer, "chunkDeserializer");
    }

    @Override
    public HttpResponse.BodySubscriber<DUUIStream<T>> apply(HttpResponse.ResponseInfo responseInfo) {
        return new Subscriber<>(chunkDeserializer);
    }

    private static final class Subscriber<T> implements HttpResponse.BodySubscriber<DUUIStream<T>> {
        private final DUUIDeserializer<T> chunkDeserializer;
        private final PrimitiveStream<T> stream = new PrimitiveStream<>();
        private final CompletableFuture<DUUIStream<T>> body = CompletableFuture.completedFuture(stream);
        private Flow.Subscription subscription;

        private Subscriber(DUUIDeserializer<T> chunkDeserializer) {
            this.chunkDeserializer = chunkDeserializer;
        }

        @Override
        public CompletionStage<DUUIStream<T>> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            stream.openConnection();
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            try {
                for (ByteBuffer item : items) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    stream.emit(chunkDeserializer.deserialize(new ByteArrayInputStream(chunk)));
                }
                subscription.request(1);
            } catch (Exception error) {
                stream.fail(error);
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            stream.fail(throwable);
        }

        @Override
        public void onComplete() {
            stream.complete();
        }
    }

    private static final class PrimitiveStream<T> implements DUUIStream<T> {
        private static final Object END = new Object();
        private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean open = new AtomicBoolean(false);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private volatile Consumer<? super T> sink;

        private void openConnection() {
            open.set(true);
        }

        private void emit(T value) {
            if (cancelled.get()) {
                return;
            }
            Consumer<? super T> currentSink = sink;
            if (currentSink != null) {
                currentSink.accept(value);
                return;
            }
            queue.offer(value);
        }

        private void complete() {
            open.set(false);
            queue.offer(END);
        }

        private void fail(Throwable throwable) {
            failure.compareAndSet(null, throwable);
            cancel();
        }

        @Override
        public Stream<T> stream() {
            Iterator<T> iterator = new Iterator<>() {
                private Object next;

                @Override
                public boolean hasNext() {
                    if (next == END) {
                        return false;
                    }
                    if (next != null) {
                        return true;
                    }
                    next = take();
                    return next != END;
                }

                @Override
                @SuppressWarnings("unchecked")
                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    Object value = next;
                    next = null;
                    return (T) value;
                }

                private Object take() {
                    Throwable error = failure.get();
                    if (error != null) {
                        throw new IllegalStateException("DUUI stream failed", error);
                    }
                    try {
                        Object value = queue.take();
                        error = failure.get();
                        if (error != null) {
                            throw new IllegalStateException("DUUI stream failed", error);
                        }
                        return value;
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting for DUUI stream chunk", interrupted);
                    }
                }
            };
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                    .onClose(this::cancel);
        }

        @Override
        public synchronized DUUIStream<T> sink(Consumer<? super T> sink) {
            this.sink = Objects.requireNonNull(sink, "sink");
            Object value;
            while ((value = queue.poll()) != null) {
                if (value == END) {
                    queue.offer(END);
                    break;
                }
                @SuppressWarnings("unchecked")
                T typed = (T) value;
                this.sink.accept(typed);
            }
            return this;
        }

        @Override
        public void cancel() {
            cancelled.set(true);
            open.set(false);
            queue.offer(END);
        }

        @Override
        public boolean cancelled() {
            return cancelled.get();
        }

        @Override
        public boolean open() {
            return open.get() && !cancelled.get();
        }
    }
}

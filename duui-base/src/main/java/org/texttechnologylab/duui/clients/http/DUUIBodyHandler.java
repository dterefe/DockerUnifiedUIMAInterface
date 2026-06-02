package org.texttechnologylab.duui.clients.http;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class DUUIBodyHandler<T> implements HttpResponse.BodyHandler<T> {
    private final DUUIDeserializer<T> deserializer;

    public DUUIBodyHandler(DUUIDeserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    @Override
    public HttpResponse.BodySubscriber<T> apply(HttpResponse.ResponseInfo responseInfo) {
        return new Subscriber<>(deserializer);
    }

    private static final class Subscriber<T> implements HttpResponse.BodySubscriber<T> {
        private final DUUIDeserializer<T> deserializer;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final CompletableFuture<T> body = new CompletableFuture<>();
        private Flow.Subscription subscription;

        private Subscriber(DUUIDeserializer<T> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public CompletionStage<T> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            try {
                for (ByteBuffer item : items) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    buffer.write(chunk);
                }
            } catch (IOException error) {
                body.completeExceptionally(error);
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            try {
                body.complete(deserializer.deserialize(new ByteArrayInputStream(buffer.toByteArray())));
            } catch (Exception error) {
                body.completeExceptionally(error);
            }
        }
    }
}

package org.texttechnologylab.duui.clients.http;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class DUUIBodyHandler<T> implements HttpResponse.BodyHandler<T> {
    @FunctionalInterface
    public interface BodyDecoder<T> {
        T decode(InputStream input) throws Exception;
    }

    private final DUUIRelay<T> relay;
    private final BodyDecoder<T> decoder;

    public DUUIBodyHandler(DUUIRelay<T> relay, BodyDecoder<T> decoder) {
        this.relay = relay;
        this.decoder = decoder;
    }

    @Override
    public HttpResponse.BodySubscriber<T> apply(HttpResponse.ResponseInfo responseInfo) {
        return new Subscriber<>(relay, decoder);
    }

    private static final class Subscriber<T> implements HttpResponse.BodySubscriber<T> {
        private final DUUIRelay<T> relay;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final BodyDecoder<T> decoder;
        private Flow.Subscription subscription;

        private Subscriber(DUUIRelay<T> relay, BodyDecoder<T> decoder) {
            this.relay = relay;
            this.decoder = decoder;
        }

        @Override
        public CompletionStage<T> getBody() {
            return relay.future();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            try {
                for (ByteBuffer item : items) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    output.write(chunk);
                }
                subscription.request(1);
            } catch (IOException error) {
                relay.cancel(error);
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            relay.cancel(throwable);
        }

        @Override
        public void onComplete() {
            try {
                T value = decoder.decode(new ByteArrayInputStream(output.toByteArray()));
                relay.complete(value);
            } catch (Exception error) {
                relay.cancel(error);
            }
        }
    }
}

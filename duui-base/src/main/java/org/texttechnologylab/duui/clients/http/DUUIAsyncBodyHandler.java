package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.exception.DUUICancellationException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class DUUIAsyncBodyHandler implements HttpResponse.BodyHandler<DUUIHttpResponse> {
    private final DUUIRelay<?> relay;

    public DUUIAsyncBodyHandler(DUUIRelay<?> relay) {
        this.relay = Objects.requireNonNull(relay, "relay");
    }

    @Override
    public HttpResponse.BodySubscriber<DUUIHttpResponse> apply(HttpResponse.ResponseInfo responseInfo) {
        return new Subscriber(relay, responseInfo);
    }

    private static final class Subscriber implements HttpResponse.BodySubscriber<DUUIHttpResponse> {
        private final DUUIRelay<?> relay;
        private final HttpResponse.ResponseInfo responseInfo;
        private final CompletableFuture<DUUIHttpResponse> body = new CompletableFuture<>();
        private OutputStream output;
        private Flow.Subscription subscription;
        private long bytes;

        private Subscriber(DUUIRelay<?> relay, HttpResponse.ResponseInfo responseInfo) {
            this.relay = relay;
            this.responseInfo = responseInfo;
        }

        @Override
        public CompletionStage<DUUIHttpResponse> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.output = relay.outputStream();
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            try {
                for (ByteBuffer item : items) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    output.write(chunk);
                    bytes += chunk.length;
                }
                subscription.request(1);
            } catch (IOException error) {
                relay.cancel(error);
                body.completeExceptionally(new DUUICancellationException(error));
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            closeQuietly(output);
            relay.cancel(throwable);
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            closeQuietly(output);
            DUUIHttpResponse response = new DUUIHttpResponse(
                    responseInfo == null ? -1 : responseInfo.statusCode(),
                    responseInfo == null ? null : responseInfo.headers(),
                    responseInfo == null ? null : responseInfo.version(),
                    bytes
            );
            body.complete(response);
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
}

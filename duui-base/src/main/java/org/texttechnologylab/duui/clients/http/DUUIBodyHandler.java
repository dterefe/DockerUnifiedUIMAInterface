package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.event.DUUILogger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class DUUIBodyHandler<T> implements HttpResponse.BodyHandler<T> {
    @FunctionalInterface
    public interface BodyDecoder<T> {
        T decode(InputStream input) throws Exception;
    }

    private final DUUIRelay<T> relay;
    private final BodyDecoder<T> decoder;
    private final DUUIEventService eventService;

    public DUUIBodyHandler(DUUIRelay<T> relay, BodyDecoder<T> decoder, DUUIEventService eventService) {
        this.relay = relay;
        this.decoder = decoder;
        this.eventService = eventService == null ? DUUIEventService.current() : eventService;
    }

    @Override
    public HttpResponse.BodySubscriber<T> apply(HttpResponse.ResponseInfo responseInfo) {
        int statusCode = responseInfo == null ? -1 : responseInfo.statusCode();
        DUUILogger logger = eventService.logger("duui.http.body");
        if (statusCode >= 500) {
            logger.critical("HTTP response body handler attached to server-error response status_code=" + statusCode + " mode=stream");
        } else if (statusCode >= 400) {
            logger.error("HTTP response body handler attached to client-error response status_code=" + statusCode + " mode=stream");
        } else if (statusCode >= 300) {
            logger.warn("HTTP response body handler attached to redirect response status_code=" + statusCode + " mode=stream");
        } else {
            logger.info("HTTP response body handler attached status_code=" + statusCode + " mode=stream");
        }
        return new Subscriber<>(relay, decoder, eventService, statusCode);
    }

    private static final class Subscriber<T> implements HttpResponse.BodySubscriber<T> {
        private final DUUIRelay<T> relay;
        private final BodyDecoder<T> decoder;
        private final DUUIEventService eventService;
        private final DUUILogger logger;
        private final int statusCode;
        private OutputStream output;
        private Flow.Subscription subscription;
        private CompletableFuture<Void> decoderTask;
        private long batches;
        private long chunks;
        private long bytes;
        private ByteArrayOutputStream errorBuffer;

        private Subscriber(DUUIRelay<T> relay, BodyDecoder<T> decoder, DUUIEventService eventService, int statusCode) {
            this.relay = relay;
            this.decoder = decoder;
            this.eventService = eventService;
            this.logger = eventService.logger("duui.http.body");
            this.statusCode = statusCode;
        }

        @Override
        public CompletionStage<T> getBody() {
            return relay.future();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            this.output = relay.outputStream();
            if (this.output == null) {
                logger.critical("HTTP response body subscriber cannot stream because relay output is null status_code=" + statusCode);
            }

            // If status code indicates an error, buffer the response body instead of decoding
            if (statusCode >= 400) {
                this.errorBuffer = new ByteArrayOutputStream(4096);
                logger.error("HTTP response error status_code=" + statusCode + " mode=stream; will capture error body");
                subscription.request(1);
                return;
            }

            logger.debug("HTTP response body subscribed; starting greedy decoder task status_code=" + statusCode + " mode=stream");
            decoderTask = CompletableFuture.runAsync(() -> {
                long started = System.currentTimeMillis();
                try {
                    logger.trace("HTTP response greedy decoder waiting for first bytes status_code=" + statusCode + " mode=stream");
                    T value = decoder.decode(relay.inputStream());
                    long durationMs = System.currentTimeMillis() - started;
                    eventService.metric("http", "duui.http.response_decode_ms", durationMs, "milliseconds", durationMs,
                            java.util.Map.of("mode", "stream"));
                    logger.info("HTTP response greedy decoder completed duration_ms=" + durationMs + " batches=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream");
                    relay.complete(value);
                } catch (Exception error) {
                    logger.error("HTTP response greedy decoder failed batches=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream", error);
                    relay.cancel(error);
                }
            });
            subscription.request(1);
            logger.trace("HTTP response body requested first upstream batch status_code=" + statusCode + " mode=stream");
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            try {
                batches++;
                long batchBytes = 0L;
                for (ByteBuffer item : items) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    if (errorBuffer != null) {
                        errorBuffer.write(chunk);
                    } else {
                        output.write(chunk);
                    }
                    chunks++;
                    bytes += chunk.length;
                    batchBytes += chunk.length;
                }
                logger.trace("HTTP response body batch forwarded batch=" + batches + " chunk_count=" + items.size() + " batch_bytes=" + batchBytes + " total_bytes=" + bytes + " mode=stream");
                subscription.request(1);
            } catch (IOException error) {
                logger.error("HTTP response body forwarding failed batch=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream", error);
                relay.cancel(error);
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("HTTP response body upstream failed batches=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream", throwable);
            closeQuietly(output);
            relay.cancel(throwable);
        }

        @Override
        public void onComplete() {
            eventService.metric("http", "duui.http.response_bytes", bytes, "bytes", 0L,
                    java.util.Map.of("mode", "stream"));

            if (errorBuffer != null) {
                // Dump the error response body
                byte[] errorBytes = errorBuffer.toByteArray();
                String bodyPreview = new String(errorBytes, 0, Math.min(errorBytes.length, 2048), StandardCharsets.UTF_8);
                System.err.println("[DUUIBodyHandler] HTTP ERROR STATUS=" + statusCode
                    + " body_bytes=" + errorBytes.length
                    + " body_preview=" + bodyPreview.replace("\n", "\\n").replace("\r", "\\r"));
                logger.error("HTTP response error body status_code=" + statusCode
                    + " body_bytes=" + errorBytes.length
                    + " body_preview=" + (bodyPreview.length() > 500 ? bodyPreview.substring(0, 500) + "..." : bodyPreview));
                relay.cancel(new IOException("HTTP error status_code=" + statusCode
                    + " body=" + bodyPreview.substring(0, Math.min(bodyPreview.length(), 200))));
                return;
            }

            if (bytes == 0L) {
                logger.warning("HTTP response body completed with zero bytes status_code=" + statusCode + " mode=stream");
            } else if (decoderTask != null && decoderTask.isDone()) {
                logger.debug("HTTP response body upstream completed after greedy decoder finished batches=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream");
            } else {
                logger.debug("HTTP response body upstream completed; closing relay output for decoder eof batches=" + batches + " chunks=" + chunks + " bytes=" + bytes + " mode=stream");
            }
            closeQuietly(output);
        }

        private static void closeQuietly(AutoCloseable closeable) {
            if (closeable == null) return;
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }
}

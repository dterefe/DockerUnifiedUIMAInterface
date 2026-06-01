package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.event.DUUIEventService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class DUUIChannel<T> {
    @FunctionalInterface
    public interface ResponseApplier<T> {
        T apply(T value, java.io.InputStream input) throws Exception;
    }

    public interface RequestCustomizer<T> {
        default URI uri(URI baseUri, T value) {
            return baseUri;
        }

        default void customize(HttpRequest.Builder builder, T value) {
        }
    }

    private final IDUUIEndpoint endpoint;
    private final DUUIHttpMethod method;
    private final String route;
    private final DUUISerializer<T> serializer;
    private final ResponseApplier<T> deserializer;
    private final RequestCustomizer<T> customizer;
    private final boolean streaming;
    private final String contentType;
    private DUUIRelay<T> responseRelay;

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer
    ) {
        this(endpoint, method, route, serializer, deserializer, new RequestCustomizer<>() {}, false);
    }

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer,
        RequestCustomizer<T> customizer
    ) {
        this(endpoint, method, route, serializer, deserializer, customizer, false);
    }

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer,
        RequestCustomizer<T> customizer,
        boolean streaming
    ) {
        this(endpoint, method, route, serializer, deserializer, customizer, streaming, "application/octet-stream");
    }

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer,
        RequestCustomizer<T> customizer,
        boolean streaming,
        String contentType
    ) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.method = Objects.requireNonNull(method, "method");
        this.route = Objects.requireNonNull(route, "route");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.customizer = Objects.requireNonNull(customizer, "customizer");
        this.streaming = streaming;
        this.contentType = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    }

    public T request(T value) throws Exception {
        if (!streaming) {
            return bufferedRequest(value);
        }
        return streamingRequest(value);
    }

    public void reset() throws IOException {
        if (responseRelay == null) {
            responseRelay = new DUUIRelay<>();
            return;
        }
        responseRelay.close();
        responseRelay.reset();
    }

    private T bufferedRequest(T value) throws Exception {
        DUUIEventService eventService = DUUIEventService.current();
        long started = System.currentTimeMillis();
        URI requestUri = customizer.uri(endpoint.uri().resolve(route), value);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        long serializeStarted = System.currentTimeMillis();
        eventService.logger("duui.http").debug("HTTP channel serialize started method=" + method + " route=" + route + " mode=buffer");
        serializer.serialize(value, body);
        long serializeMs = System.currentTimeMillis() - serializeStarted;
        byte[] requestBytes = body.toByteArray();
        eventService.metric("http", "duui.http.serialize_ms", serializeMs, "milliseconds", serializeMs,
                java.util.Map.of("route", route, "method", method.name(), "mode", "buffer"));
        eventService.metric("http", "duui.http.request_bytes", requestBytes.length, "bytes", System.currentTimeMillis() - started,
                java.util.Map.of("route", route, "method", method.name(), "mode", "buffer"));

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(requestUri)
            .version(HttpClient.Version.HTTP_1_1)
            .header("Content-Type", contentType)
            .method(method.name(), HttpRequest.BodyPublishers.ofByteArray(requestBytes));
        customizer.customize(builder, value);
        HttpRequest request = builder.build();

        try {
            eventService.logger("duui.http").info("HTTP channel request buffered method=" + method + " route=" + route + " uri=" + requestUri);
            long receiveStarted = System.currentTimeMillis();
            HttpResponse<byte[]> response = endpoint.client().sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).join();
            long receiveMs = System.currentTimeMillis() - receiveStarted;
            eventService.metric("http", "duui.http.response_receive_ms", receiveMs, "milliseconds", receiveMs,
                    java.util.Map.of("mode", "buffer"));
            eventService.metric("http", "duui.http.response_bytes", response.body().length, "bytes", receiveMs,
                    java.util.Map.of("mode", "buffer"));
            long decodeStarted = System.currentTimeMillis();
            T decoded = deserializer.apply(value, new ByteArrayInputStream(response.body()));
            long decodeMs = System.currentTimeMillis() - decodeStarted;
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.response_decode_ms", decodeMs, "milliseconds", decodeMs,
                    java.util.Map.of("mode", "buffer"));
            eventService.metric("http", "duui.http.request_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "success"));
            eventService.logger("duui.http").info("HTTP channel request completed method=" + method + " route=" + route + " receive_ms=" + receiveMs + " decode_ms=" + decodeMs + " duration_ms=" + durationMs + " mode=buffer");
            return decoded;
        } catch (Exception error) {
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.request_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "failed"));
            eventService.logger("duui.http").error("HTTP channel request failed method=" + method + " route=" + route + " duration_ms=" + durationMs + " mode=buffer", error);
            throw error;
        }
    }

    private T streamingRequest(T value) throws Exception {
        DUUIEventService eventService = DUUIEventService.current();
        long started = System.currentTimeMillis();
        eventService.logger("duui.http").debug("HTTP channel streaming serialize scheduled method=" + method + " route=" + route + " endpoint=" + endpoint.uri());
        StreamingRequestBody streamingBody = streamingRequestBody(value, started, eventService);

        reset();
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(responseRelay, input -> deserializer.apply(value, input), eventService);

        URI requestUri = customizer.uri(endpoint.uri().resolve(route), value);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(requestUri)
            .version(HttpClient.Version.HTTP_1_1)
            .header("Content-Type", contentType)
            .method(method.name(), HttpRequest.BodyPublishers.ofInputStream(() -> streamingBody.input));
        customizer.customize(builder, value);
        HttpRequest request = builder.build();

        try {
            eventService.logger("duui.http").info("HTTP channel request streaming method=" + method + " route=" + route + " uri=" + requestUri);
            CompletableFuture<HttpResponse<T>> responseFuture = endpoint.client().sendAsync(request, handler);
            responseFuture.whenComplete((response, error) -> {
                if (error != null) {
                    responseRelay.cancel(error);
                }
            });
            streamingBody.serializerFuture.join();
            T response = responseRelay.future().join();
            responseFuture.join();
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.request_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "success"));
            eventService.logger("duui.http").info("HTTP channel request completed method=" + method + " route=" + route + " duration_ms=" + durationMs);
            return response;
        } catch (Exception error) {
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.request_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "failed"));
            eventService.logger("duui.http").error("HTTP channel request failed method=" + method + " route=" + route + " duration_ms=" + durationMs, error);
            throw error;
        }
    }

    private StreamingRequestBody streamingRequestBody(T value, long requestStartedMs, DUUIEventService eventService) throws IOException {
        PipedInputStream input = new PipedInputStream(1024 * 1024);
        PipedOutputStream pipeOutput = new PipedOutputStream(input);
        CountingOutputStream countingOutput = new CountingOutputStream(pipeOutput);
        CompletableFuture<Void> serializerFuture = CompletableFuture.runAsync(() -> {
            long serializeStarted = System.currentTimeMillis();
            eventService.logger("duui.http").debug("HTTP channel serialize started method=" + method + " route=" + route + " mode=stream");
            try (CountingOutputStream output = countingOutput) {
                serializer.serialize(value, output);
                long serializeMs = System.currentTimeMillis() - serializeStarted;
                eventService.metric("http", "duui.http.serialize_ms", serializeMs, "milliseconds", serializeMs,
                        java.util.Map.of("route", route, "method", method.name(), "mode", "stream"));
                eventService.metric("http", "duui.http.request_bytes", output.bytesWritten(), "bytes", System.currentTimeMillis() - requestStartedMs,
                        java.util.Map.of("route", route, "method", method.name(), "mode", "stream"));
                eventService.logger("duui.http").debug("HTTP channel serialize completed method=" + method + " route=" + route + " bytes=" + output.bytesWritten() + " duration_ms=" + serializeMs + " mode=stream");
            } catch (Exception error) {
                closeQuietly(pipeOutput);
                throw new RuntimeException(error);
            }
        });
        return new StreamingRequestBody(input, serializerFuture);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private record StreamingRequestBody(PipedInputStream input, CompletableFuture<Void> serializerFuture) {}

    private static final class CountingOutputStream extends FilterOutputStream {
        private long bytesWritten;

        private CountingOutputStream(OutputStream output) {
            super(output);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            bytesWritten++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            bytesWritten += len;
        }

        long bytesWritten() {
            return bytesWritten;
        }
    }
}

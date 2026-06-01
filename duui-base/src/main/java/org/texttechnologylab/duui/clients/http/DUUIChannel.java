package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.timelines.DUUIStatus;
import org.texttechnologylab.duui.timelines.Phase;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
        this.contentType = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    }

    public void reset() throws IOException {
        if (responseRelay == null) {
            responseRelay = new DUUIRelay<>();
            return;
        }
        responseRelay.close();
        responseRelay.reset();
    }

    public T get() throws Exception {
        return get(null);
    }

    public T get(T value) throws Exception {
        DUUIEventService eventService = DUUIEventService.current();
        reset();
        URI requestUri = customizer.uri(endpoint.uri().resolve(route), value);
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(responseRelay, input -> cast(DUUIChannelPhaseDispatch.deserialize(this, value, input)), eventService);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(requestUri)
            .version(HttpClient.Version.HTTP_1_1)
            .GET();
        customizer.customize(builder, value);
        CompletableFuture<HttpResponse<T>> response = cast(DUUIChannelPhaseDispatch.analyseAsync(this, builder.build(), handler));
        response.whenComplete((ignored, error) -> {
            if (error != null) {
                responseRelay.cancel(error);
            }
        });
        return responseRelay.future().join();
    }

    public CompletableFuture<HttpResponse<InputStream>> sseGet() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint.uri().resolve(route))
                .version(HttpClient.Version.HTTP_1_1)
                .header("Accept", "text/event-stream")
                .GET()
                .build();
        return endpoint.client().sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    public T post(T value) throws Exception {
        DUUIEventService eventService = DUUIEventService.current();
        eventService.logger("duui.http").debug("HTTP channel POST scheduled route=" + route + " endpoint=" + endpoint.uri());
        StreamingRequestBody body = requestBody(value);
        reset();
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(responseRelay, input -> cast(DUUIChannelPhaseDispatch.deserialize(this, value, input)), eventService);
        URI requestUri = customizer.uri(endpoint.uri().resolve(route), value);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(requestUri)
                .version(HttpClient.Version.HTTP_1_1)
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> body.input));
        customizer.customize(builder, value);
        try {
            eventService.logger("duui.http").info("HTTP channel POST route=" + route + " uri=" + requestUri);
            CompletableFuture<HttpResponse<T>> responseFuture = cast(DUUIChannelPhaseDispatch.analyseAsync(this, builder.build(), handler));
            responseFuture.whenComplete((response, error) -> {
                if (error != null) {
                    responseRelay.cancel(error);
                }
            });
            body.serializerFuture.join();
            T response = responseRelay.future().join();
            responseFuture.join();
            return response;
        } catch (Exception error) {
            eventService.logger("duui.http").error("HTTP channel POST failed route=" + route, error);
            throw error;
        }
    }

    @Phase(value = DUUIStatus.SERIALIZE, dispatch = DUUIDispatchMode.IO)
    public void serialize(Object value, OutputStream output) throws Exception {
        serializer.serialize(cast(value), output);
    }

    @Phase(value = DUUIStatus.ANALYSE, dispatch = DUUIDispatchMode.IO)
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<HttpResponse> analyse(HttpRequest request, HttpResponse.BodyHandler handler) {
        return endpoint.client().sendAsync(request, handler);
    }

    @Phase(value = DUUIStatus.DESERIALIZE, dispatch = DUUIDispatchMode.CPU)
    public Object deserialize(Object value, InputStream input) throws Exception {
        return deserializer.apply(cast(value), input);
    }

    private StreamingRequestBody requestBody(T value) throws IOException {
        PipedInputStream input = new PipedInputStream(1024 * 1024);
        PipedOutputStream pipeOutput = new PipedOutputStream(input);
        CountingOutputStream countingOutput = new CountingOutputStream(pipeOutput);
        CompletableFuture<Void> serializerFuture = DUUIChannelPhaseDispatch.serializeAsync(this, value, countingOutput)
                .whenComplete((ignored, error) -> closeQuietly(error == null ? countingOutput : pipeOutput));
        return new StreamingRequestBody(input, serializerFuture);
    }

    @SuppressWarnings("unchecked")
    private static <V> V cast(Object value) {
        return (V) value;
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

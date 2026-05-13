package org.texttechnologylab.duui.clients.http;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Objects;

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

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer
    ) {
        this(endpoint, method, route, serializer, deserializer, new RequestCustomizer<>() {});
    }

    public DUUIChannel(
        IDUUIEndpoint endpoint,
        DUUIHttpMethod method,
        String route,
        DUUISerializer<T> serializer,
        ResponseApplier<T> deserializer,
        RequestCustomizer<T> customizer
    ) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.method = Objects.requireNonNull(method, "method");
        this.route = Objects.requireNonNull(route, "route");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.customizer = Objects.requireNonNull(customizer, "customizer");
    }

    public T request(T value) throws Exception {
        ByteArrayOutputStream requestBody = new ByteArrayOutputStream();
        serializer.serialize(value, requestBody);

        DUUIRelay<T> responseRelay = new DUUIRelay<>();
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(responseRelay, input -> deserializer.apply(value, input));

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(customizer.uri(endpoint.uri().resolve(route), value))
            .version(HttpClient.Version.HTTP_1_1)
            .header("Content-Type", "application/octet-stream")
            .method(method.name(), HttpRequest.BodyPublishers.ofByteArray(requestBody.toByteArray()));
        customizer.customize(builder, value);
        HttpRequest request = builder.build();

        try {
            endpoint.client().send(request, handler);
        } catch (Exception error) {
            throw error;
        }
        return responseRelay.future().join();
    }
}

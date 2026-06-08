package org.texttechnologylab.duui.clients.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;

public final class DUUIChannel<T> {
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
    private final RequestCustomizer<T> customizer;
    private final String contentType;

    public DUUIChannel(IDUUIEndpoint endpoint, DUUIHttpMethod method, String route) {
        this(endpoint, method, route, new RequestCustomizer<>() {}, "application/octet-stream");
    }

    public DUUIChannel(
            IDUUIEndpoint endpoint,
            DUUIHttpMethod method,
            String route,
            RequestCustomizer<T> customizer,
            String contentType
    ) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.method = Objects.requireNonNull(method, "method");
        this.route = Objects.requireNonNull(route, "route");
        this.customizer = Objects.requireNonNull(customizer, "customizer");
        this.contentType = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    }

    public DUUIHttpResponse request(DUUIRelay<T> serializationRelay, DUUIRelay<DUUIHttpResponse> deserializationRelay) throws Exception {
        Objects.requireNonNull(serializationRelay, "serializationRelay");
        Objects.requireNonNull(deserializationRelay, "deserializationRelay");
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(customizer.uri(endpoint.uri().resolve(route), null))
                .version(HttpClient.Version.HTTP_1_1)
                .method(method.name(), HttpRequest.BodyPublishers.ofInputStream(serializationRelay::inputStream));
        builder.header("Content-Type", contentType);
        customizer.customize(builder, null);
        DUUIAsyncBodyHandler bodyHandler = new DUUIAsyncBodyHandler(deserializationRelay);
        HttpResponse<DUUIHttpResponse> response = endpoint.client().send(builder.build(), bodyHandler);
        return response.body();
    }
}

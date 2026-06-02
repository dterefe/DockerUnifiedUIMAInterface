package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.event.DUUILogger;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;

public final class DUUISignal<T> {
    private final IDUUIEndpoint endpoint;
    private final DUUIHttpMethod method;
    private final String route;
    private final HttpResponse.BodyHandler<T> bodyHandler;

    public DUUISignal(IDUUIEndpoint endpoint, DUUIHttpMethod method, String route, DUUIDeserializer<T> deserializer) {
        this(endpoint, method, route, new DUUIBodyHandler<>(deserializer));
    }

    public DUUISignal(IDUUIEndpoint endpoint, DUUIHttpMethod method, String route, HttpResponse.BodyHandler<T> bodyHandler) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.method = Objects.requireNonNull(method, "method");
        this.route = Objects.requireNonNull(route, "route");
        this.bodyHandler = Objects.requireNonNull(bodyHandler, "bodyHandler");
    }

    public T request() throws Exception {
        long started = System.currentTimeMillis();
        DUUILogger logger = DUUILogger.get("duui.http");
        logger.debug("HTTP signal request started", "method=" + method, "route=" + route, "endpoint=" + endpoint.uri());
        HttpRequest request = HttpRequest.newBuilder()
            .uri(endpoint.uri().resolve(route))
            .version(HttpClient.Version.HTTP_1_1)
            .method(method.name(), HttpRequest.BodyPublishers.noBody())
            .build();

        try {
            HttpResponse<T> httpResponse = endpoint.client().send(request, bodyHandler);
            T response = httpResponse.body();
            long durationMs = System.currentTimeMillis() - started;
            logger.debug("HTTP signal request completed", "method=" + method, "route=" + route, "duration_ms=" + durationMs);
            return response;
        } catch (Exception error) {
            long durationMs = System.currentTimeMillis() - started;
            logger.error("HTTP signal request failed", error, "method=" + method, "route=" + route, "duration_ms=" + durationMs);
            throw error;
        }
    }
}

package org.texttechnologylab.duui.clients.http;

import org.texttechnologylab.duui.event.DUUIEventService;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Objects;

public final class DUUISignal<T> {
    private final IDUUIEndpoint endpoint;
    private final DUUIHttpMethod method;
    private final String route;
    private final DUUIDeserializer<T> deserializer;

    public DUUISignal(IDUUIEndpoint endpoint, DUUIHttpMethod method, String route, DUUIDeserializer<T> deserializer) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.method = Objects.requireNonNull(method, "method");
        this.route = Objects.requireNonNull(route, "route");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
    }

    public T request() throws Exception {
        long started = System.currentTimeMillis();
        DUUIEventService eventService = DUUIEventService.current();
        eventService.logger("duui.http").debug("HTTP signal request started method=" + method + " route=" + route + " endpoint=" + endpoint.uri());
        DUUIRelay<T> relay = new DUUIRelay<>();
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(relay, deserializer::deserialize, eventService);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(endpoint.uri().resolve(route))
            .version(HttpClient.Version.HTTP_1_1)
            .method(method.name(), HttpRequest.BodyPublishers.noBody())
            .build();

        try {
            endpoint.client().send(request, handler);
            T response = relay.future().join();
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.signal_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "success"));
            eventService.logger("duui.http").debug("HTTP signal request completed method=" + method + " route=" + route + " duration_ms=" + durationMs);
            return response;
        } catch (Exception error) {
            long durationMs = System.currentTimeMillis() - started;
            eventService.metric("http", "duui.http.signal_duration_ms", durationMs, "milliseconds", durationMs,
                    java.util.Map.of("route", route, "method", method.name(), "status", "failed"));
            eventService.logger("duui.http").error("HTTP signal request failed method=" + method + " route=" + route + " duration_ms=" + durationMs, error);
            throw error;
        }
    }
}

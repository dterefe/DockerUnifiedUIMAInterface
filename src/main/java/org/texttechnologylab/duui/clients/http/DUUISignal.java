package org.texttechnologylab.duui.clients.http;

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
        DUUIRelay<T> relay = new DUUIRelay<>();
        DUUIBodyHandler<T> handler = new DUUIBodyHandler<>(relay, deserializer::deserialize);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(endpoint.uri().resolve(route))
            .version(HttpClient.Version.HTTP_1_1)
            .method(method.name(), HttpRequest.BodyPublishers.noBody())
            .build();

        endpoint.client().send(request, handler);
        return relay.future().join();
    }
}

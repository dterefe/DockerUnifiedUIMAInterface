package org.texttechnologylab.duui.clients.hosts.remote;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;

public final class DUUIRemoteEnvironment {
    private final HttpClient client;

    public DUUIRemoteEnvironment() {
        this(HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .version(HttpClient.Version.HTTP_1_1)
                .build());
    }

    public DUUIRemoteEnvironment(HttpClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public DUUIRemoteEndpoint endpoint(String baseUri) {
        return new DUUIRemoteEndpoint(DUUIAddress.parse(baseUri), client);
    }
}

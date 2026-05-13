package org.texttechnologylab.duui.clients.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Objects;

public record DUUIHttpEndpoint(URI uri, HttpClient client) implements IDUUIEndpoint {
    public DUUIHttpEndpoint {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(client, "client");
    }
}

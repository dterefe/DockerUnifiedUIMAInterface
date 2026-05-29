package org.texttechnologylab.duui.clients.hosts.remote;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Objects;

public record DUUIRemoteEndpoint(DUUIAddress address, HttpClient client) implements DUUIProxy, IDUUIEndpoint {
    public DUUIRemoteEndpoint {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(client, "client");
    }

    @Override
    public URI uri() {
        return address.uri();
    }
}

package org.texttechnologylab.duui.clients.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

public final class DUUIHttpClient {
    public IDUUIEndpoint endpoint(Descriptor descriptor) {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(descriptor.connectTimeoutSeconds()))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
        return new DUUIHttpEndpoint(URI.create(descriptor.baseUri()), client);
    }

    public record Descriptor(
        String baseUri,
        int connectTimeoutSeconds,
        int requestTimeoutSeconds,
        int channels,
        int requestPipeBufferBytes,
        int responsePipeBufferBytes,
        int initialReplayCapacityBytes
    ) {
    }
}

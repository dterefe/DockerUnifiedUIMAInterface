package org.texttechnologylab.duui.clients.http;

import java.net.http.HttpHeaders;
import java.net.http.HttpClient;

public record DUUIHttpResponse(
        int statusCode,
        HttpHeaders headers,
        HttpClient.Version version,
        long bodyBytes
) {
}

package org.texttechnologylab.duui.gateway.validation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.gateway.model.GatewayAnnotatorRegistration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class V1AnnotatorValidator {
    private final HttpClient client;
    private final ObjectMapper mapper;

    public V1AnnotatorValidator(ObjectMapper mapper) {
        this.mapper = mapper;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public GatewayAnnotatorRegistration validate(GatewayAnnotatorRegistration requested) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> descriptor = new LinkedHashMap<>();
        String endpoint = stripTrailingSlash(requested.endpoint());
        fetchJson(endpoint + "/v1/documentation", "documentation", descriptor, errors);
        fetchText(endpoint + "/v1/typesystem", "typesystem", descriptor, errors);
        fetchJson(endpoint + "/v1/communication_layer", "communicationLayer", descriptor, errors);
        String status = errors.isEmpty() ? "validated" : "invalid";
        return new GatewayAnnotatorRegistration(
                requested.id(),
                requested.name(),
                endpoint,
                requested.environment(),
                requested.image(),
                status,
                descriptor,
                Instant.now(),
                errors,
                requested.tags()
        );
    }

    private void fetchJson(String url, String key, Map<String, Object> target, List<String> errors) {
        try {
            HttpResponse<String> response = client.send(request(url), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                errors.add(key + " returned HTTP " + response.statusCode());
                return;
            }
            target.put(key, mapper.readValue(response.body(), new TypeReference<Map<String, Object>>() {}));
        } catch (Exception error) {
            errors.add(key + " failed: " + error.getMessage());
        }
    }

    private void fetchText(String url, String key, Map<String, Object> target, List<String> errors) {
        try {
            HttpResponse<String> response = client.send(request(url), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                errors.add(key + " returned HTTP " + response.statusCode());
                return;
            }
            target.put(key, Map.of("xml", response.body(), "bytes", response.body().getBytes(java.nio.charset.StandardCharsets.UTF_8).length));
        } catch (Exception error) {
            errors.add(key + " failed: " + error.getMessage());
        }
    }

    private static HttpRequest request(String url) {
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .GET()
                .build();
    }

    private static String stripTrailingSlash(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Annotator endpoint must not be blank");
        }
        String stripped = value.strip();
        while (stripped.endsWith("/")) {
            stripped = stripped.substring(0, stripped.length() - 1);
        }
        return stripped;
    }
}

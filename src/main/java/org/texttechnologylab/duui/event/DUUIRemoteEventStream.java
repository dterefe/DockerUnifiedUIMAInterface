package org.texttechnologylab.duui.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUIRemoteEventStream implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final IDUUIEndpoint endpoint;
    private final DUUIEventService service;
    private final String streamId;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<HttpResponse<Void>> subscription;

    private DUUIRemoteEventStream(IDUUIEndpoint endpoint, DUUIEventService service, String streamId) {
        this.endpoint = endpoint;
        this.service = service;
        this.streamId = streamId;
        this.subscription = subscribe();
    }

    public static DUUIRemoteEventStream connect(
            IDUUIEndpoint endpoint,
            DUUIV1TelemetryConfig config,
            String annotatorId
    ) {
        Objects.requireNonNull(endpoint, "endpoint");
        DUUIEventService service = service(config);
        if (config == null || !config.enabled()) return null;
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("annotator_id", annotatorId);
            body.put("replica_id", annotatorId);
            body.put("request_id", UUID.randomUUID().toString());
            body.put("ttl_minutes", config.ttlMinutes());
            HttpRequest request = HttpRequest.newBuilder(resolve(endpoint.uri(), "/v2/events/connect"))
                    .timeout(Duration.ofSeconds(5))
                    .version(java.net.http.HttpClient.Version.HTTP_1_1)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = endpoint.client().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                        .name("remote-events")
                        .status(DUUIEventStatus.UNSUPPORTED)
                        .message("Remote event endpoint returned HTTP " + response.statusCode())
                        .build());
                return null;
            }
            Map<String, Object> responseBody = MAPPER.readValue(response.body(), new TypeReference<>() {});
            Object id = responseBody.get("stream_id");
            if (id == null || id.toString().isBlank()) {
                service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                        .name("remote-events")
                        .status(DUUIEventStatus.UNSUPPORTED)
                        .message("Remote event endpoint did not return a stream id.")
                        .build());
                return null;
            }
            service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                    .name("remote-events")
                    .status(DUUIEventStatus.STARTED)
                    .message("Connected remote event stream " + id)
                    .build());
            return new DUUIRemoteEventStream(endpoint, service, id.toString());
        } catch (Exception error) {
            service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                    .name("remote-events")
                    .status(DUUIEventStatus.UNSUPPORTED)
                    .message(error.getMessage())
                    .build());
            return null;
        }
    }

    private CompletableFuture<HttpResponse<Void>> subscribe() {
        HttpRequest request = HttpRequest.newBuilder(resolve(endpoint.uri(), "/v2/events/stream?stream_id=" + streamId))
                .timeout(Duration.ofSeconds(configuredStreamTimeoutSeconds()))
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .GET()
                .build();
        CompletableFuture<HttpResponse<Void>> future = endpoint.client().sendAsync(request, responseInfo -> {
            if (responseInfo.statusCode() < 200 || responseInfo.statusCode() >= 300) {
                service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                        .name("remote-events")
                        .status(DUUIEventStatus.UNSUPPORTED)
                        .message("Remote event stream returned HTTP " + responseInfo.statusCode())
                        .build());
                return HttpResponse.BodySubscribers.replacing(null);
            }
            service.logger("remote-events").info("Remote event stream subscribed: " + streamId);
            return HttpResponse.BodySubscribers.fromLineSubscriber(new SseLineSubscriber(this::emitSsePayload));
        });
        future.whenComplete((ignored, error) -> {
            if (error != null && !closed.get()) {
                service.error("remote-events", error, null);
            }
        });
        return future;
    }

    private void emitSsePayload(String json) {
        if (closed.get() || json == null || json.isBlank()) return;
        try {
            service.emit(mapRemoteEvent(json));
        } catch (Exception error) {
            service.error("remote-events", error, null);
        }
    }

    private DUUIEvent mapRemoteEvent(String json) throws Exception {
        Map<String, Object> values = MAPPER.readValue(json, new TypeReference<>() {});
        String type = string(values.get("type"));
        Map<String, Object> remoteContext = map(values.get("context"));
        DUUIEventContext context = new DUUIEventContext(
                trace(remoteContext),
                string(remoteContext.get("orchestrator_id")),
                string(remoteContext.get("task_id")),
                string(remoteContext.get("artifact_id")),
                string(remoteContext.get("checkpoint_id")),
                string(remoteContext.get("stage_id")),
                string(remoteContext.get("component_id")),
                string(remoteContext.get("node_id")),
                string(remoteContext.get("annotator_id")),
                string(remoteContext.get("worker_id")),
                string(remoteContext.get("phase_id")),
                string(remoteContext.get("phase_status")),
                string(remoteContext.get("phase_lifecycle"))
        );
        if ("metric".equalsIgnoreCase(type)) {
            return DUUIEvent.builder(DUUIEventType.METRIC)
                    .context(context)
                    .name(string(values.get("category")))
                    .metric(string(values.get("name")), number(values.get("value")), string(values.get("unit")))
                    .metricTags(stringMap(values.get("tags")))
                    .attributes(values)
                    .build();
        }
        if ("error".equalsIgnoreCase(type)) {
            return DUUIEvent.builder(DUUIEventType.ERROR)
                    .context(context)
                    .name("remote-error")
                    .level(DUUIEventLevel.ERROR)
                    .message(string(values.get("message")))
                    .error(string(values.get("error_type")), string(values.get("stack_trace")), string(values.get("recovery_suggestion")))
                    .attributes(values)
                    .build();
        }
        return DUUIEvent.builder(DUUIEventType.LOG)
                .context(context)
                .name("remote-log")
                .level(level(string(values.get("level"))))
                .message(string(values.get("message")))
                .attributes(values)
                .build();
    }

    private static DUUITraceContext trace(Map<String, Object> context) {
        String traceId = string(context.get("trace_id"));
        String spanId = string(context.get("span_id"));
        if (traceId == null || spanId == null) return DUUITraceContext.root();
        try {
            return new DUUITraceContext(traceId, spanId, string(context.get("parent_span_id")));
        } catch (IllegalArgumentException ignored) {
            return DUUITraceContext.root();
        }
    }

    private static DUUIEventLevel level(String level) {
        if (level == null) return DUUIEventLevel.INFO;
        return switch (level.toLowerCase()) {
            case "trace" -> DUUIEventLevel.TRACE;
            case "debug" -> DUUIEventLevel.DEBUG;
            case "warn", "warning" -> DUUIEventLevel.WARN;
            case "error" -> DUUIEventLevel.ERROR;
            default -> DUUIEventLevel.INFO;
        };
    }

    private static URI resolve(URI base, String route) {
        String root = base.toString();
        if (!root.endsWith("/")) root += "/";
        return URI.create(root).resolve(route.startsWith("/") ? route.substring(1) : route);
    }

    private static DUUIEventService service(DUUIV1TelemetryConfig config) {
        if (config != null && config.sink() != null) {
            return new DUUIEventService(java.util.List.of(config.sink()));
        }
        return DUUIEventService.current();
    }

    private static long configuredStreamTimeoutSeconds() {
        return 60L * 60L;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private static Map<String, String> stringMap(Object value) {
        Map<String, Object> input = map(value);
        Map<String, String> output = new LinkedHashMap<>();
        input.forEach((key, item) -> {
            if (item != null) output.put(key, item.toString());
        });
        return output;
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }

    private static Double number(Object value) {
        if (value instanceof Number number) return number.doubleValue();
        if (value == null) return null;
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        subscription.cancel(true);
        try {
            endpoint.client().send(HttpRequest.newBuilder(resolve(endpoint.uri(), "/v2/events/" + streamId))
                    .version(java.net.http.HttpClient.Version.HTTP_1_1)
                    .DELETE()
                    .build(), HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
        }
    }

    private static final class SseLineSubscriber implements Flow.Subscriber<String> {
        private final java.util.function.Consumer<String> consumer;
        private Flow.Subscription subscription;

        private SseLineSubscriber(java.util.function.Consumer<String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(String line) {
            if (line != null && line.startsWith("data:")) {
                consumer.accept(line.substring("data:".length()).trim());
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    }
}

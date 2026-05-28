package org.texttechnologylab.duui.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.clients.http.IDUUIEndpoint;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUIRemoteEventStream implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final IDUUIEndpoint endpoint;
    private final DUUIEventService service;
    private final URI streamUri;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CompletableFuture<HttpResponse<Void>> subscription;

    private DUUIRemoteEventStream(IDUUIEndpoint endpoint, DUUIEventService service, URI streamUri) {
        this.endpoint = endpoint;
        this.service = service;
        this.streamUri = streamUri;
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
            URI streamUri = eventStreamUri(endpoint.uri(), config, annotatorId);
            service.emit(DUUIEvent.builder(DUUIEventType.STATUS)
                    .name("remote-events")
                    .status(DUUIEventStatus.STARTED)
                    .message("Opening remote event stream " + streamUri)
                    .build());
            return new DUUIRemoteEventStream(endpoint, service, streamUri);
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
        HttpRequest request = HttpRequest.newBuilder(streamUri)
                .timeout(Duration.ofSeconds(configuredStreamTimeoutSeconds()))
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .header("Accept", "text/event-stream")
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
            service.logger("remote-events").info("Remote event stream subscribed: " + streamUri);
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
        if (type == null) {
            type = string(values.get("event"));
        }
        if (type == null && values.containsKey("stream_id")) {
            type = "handshake";
        }
        DUUIEventContext context = context(values);
        if ("metric".equalsIgnoreCase(type)) {
            Map<String, Object> point = firstMap(values.get("data_points"));
            Map<String, String> tags = stringMap(values.get("tags"));
            tags.putAll(stringMap(values.get("attributes")));
            tags.putAll(stringMap(point.get("attributes")));
            return DUUIEvent.builder(DUUIEventType.METRIC)
                    .context(context)
                    .name(firstPresent(attribute(values, "duui.metric.category"), string(values.get("metric_type")), "remote-metric"))
                    .metric(string(values.get("name")), firstNumber(values, point), string(values.get("unit")))
                    .metricTags(tags)
                    .attributes(values)
                    .build();
        }
        if ("error".equalsIgnoreCase(type)) {
            return DUUIEvent.builder(DUUIEventType.ERROR)
                    .context(context)
                    .name("remote-error")
                    .level(DUUIEventLevel.ERROR)
                    .message(firstPresent(string(values.get("body")), string(values.get("message"))))
                    .error(string(values.get("error_type")), string(values.get("stack_trace")), string(values.get("recovery_suggestion")))
                    .attributes(values)
                    .build();
        }
        if ("handshake".equalsIgnoreCase(type)) {
            return DUUIEvent.builder(DUUIEventType.STATUS)
                    .context(context)
                    .name("remote-events")
                    .status(DUUIEventStatus.STARTED)
                    .message("Remote event stream handshake received")
                    .attributes(values)
                    .build();
        }
        return DUUIEvent.builder(DUUIEventType.LOG)
                .context(context)
                .name("remote-log")
                .level(level(firstPresent(string(values.get("severity_text")), string(values.get("level")))))
                .message(firstPresent(string(values.get("body")), string(values.get("message")), string(values.get("name"))))
                .attributes(values)
                .build();
    }

    private static DUUIEventContext context(Map<String, Object> values) {
        Map<String, Object> remoteContext = map(values.get("context"));
        Map<String, Object> attributes = map(values.get("attributes"));
        Map<String, Object> resource = map(values.get("resource"));
        return new DUUIEventContext(
                trace(values, remoteContext, attributes),
                firstPresent(attribute(attributes, resource, "duui.orchestrator_id"), string(remoteContext.get("orchestrator_id"))),
                firstPresent(attribute(attributes, resource, "duui.pipeline_run_id"), string(remoteContext.get("task_id"))),
                firstPresent(attribute(attributes, resource, "duui.artifact_id"), string(remoteContext.get("artifact_id"))),
                firstPresent(attribute(attributes, resource, "duui.checkpoint_id"), string(remoteContext.get("checkpoint_id"))),
                firstPresent(attribute(attributes, resource, "duui.stage_id"), string(remoteContext.get("stage_id"))),
                firstPresent(attribute(attributes, resource, "duui.component_id"), string(remoteContext.get("component_id"))),
                firstPresent(attribute(attributes, resource, "duui.replica_id"), string(remoteContext.get("node_id"))),
                firstPresent(attribute(attributes, resource, "duui.annotator_id"), string(remoteContext.get("annotator_id"))),
                firstPresent(attribute(attributes, resource, "duui.machine_id"), string(remoteContext.get("worker_id"))),
                string(remoteContext.get("phase_id")),
                string(remoteContext.get("phase_status")),
                string(remoteContext.get("phase_lifecycle"))
        );
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

    private static DUUITraceContext trace(Map<String, Object> values, Map<String, Object> context, Map<String, Object> attributes) {
        String traceId = firstPresent(string(values.get("trace_id")), string(context.get("trace_id")), string(attributes.get("trace_id")));
        String spanId = firstPresent(string(values.get("span_id")), string(context.get("span_id")), string(attributes.get("span_id")));
        String parentSpanId = firstPresent(string(values.get("parent_span_id")), string(context.get("parent_span_id")), string(attributes.get("parent_span_id")));
        if (traceId == null || spanId == null) return DUUITraceContext.root();
        try {
            return new DUUITraceContext(traceId, spanId, parentSpanId);
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

    private static URI eventStreamUri(URI base, DUUIV1TelemetryConfig config, String annotatorId) {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("ttl_minutes", String.valueOf(config.ttlMinutes()));
        query.put("annotator_id", annotatorId);
        query.put("replica_id", annotatorId);
        return URI.create(resolve(base, "/v2/events") + "?" + query(query));
    }

    private static String query(Map<String, String> values) {
        return values.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isBlank())
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                .collect(java.util.stream.Collectors.joining("&"));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
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

    private static Map<String, Object> firstMap(Object value) {
        if (value instanceof List<?> list && !list.isEmpty()) {
            return map(list.get(0));
        }
        return Map.of();
    }

    private static Double firstNumber(Map<String, Object> values, Map<String, Object> point) {
        Double value = number(values.get("value"));
        if (value != null) return value;
        for (String key : List.of("as_double", "count", "sum", "value")) {
            value = number(point.get(key));
            if (value != null) return value;
        }
        return null;
    }

    private static String attribute(Map<String, Object> values, String key) {
        return string(map(values.get("attributes")).get(key));
    }

    private static String attribute(Map<String, Object> attributes, Map<String, Object> resource, String key) {
        return firstPresent(string(attributes.get(key)), string(resource.get(key)));
    }

    private static String firstPresent(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
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

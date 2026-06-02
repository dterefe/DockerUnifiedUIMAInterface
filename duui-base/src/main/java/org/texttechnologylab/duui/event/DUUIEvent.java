package org.texttechnologylab.duui.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.clients.http.DUUIDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DUUIEvent {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String id;
    private final Instant timestamp;
    private final DUUIEventType type;
    private final DUUIEventLevel level;
    private final DUUIEventStatus status;
    private final DUUIEventContext context;
    private final String name;
    private final String message;
    private final String metricName;
    private final Double metricValue;
    private final String metricUnit;
    private final long metricIntervalMs;
    private final Map<String, String> metricTags;
    private final String errorType;
    private final String stackTrace;
    private final String recoveryHint;
    private final Map<String, Object> attributes;

    private DUUIEvent(Builder builder) {
        this.id = builder.id == null ? UUID.randomUUID().toString() : builder.id;
        this.timestamp = builder.timestamp == null ? Instant.now() : builder.timestamp;
        this.type = Objects.requireNonNull(builder.type, "type");
        this.level = builder.level;
        this.status = builder.status;
        this.context = builder.context == null ? new DUUIEventContext(null, null, null, null, null, null, null, null, null, null) : builder.context;
        this.name = builder.name;
        this.message = builder.message;
        this.metricName = builder.metricName;
        this.metricValue = builder.metricValue;
        this.metricUnit = builder.metricUnit;
        this.metricIntervalMs = builder.metricIntervalMs;
        this.metricTags = Map.copyOf(builder.metricTags);
        this.errorType = builder.errorType;
        this.stackTrace = builder.stackTrace;
        this.recoveryHint = builder.recoveryHint;
        this.attributes = Map.copyOf(builder.attributes);
    }

    public static Builder builder(DUUIEventType type) {
        return new Builder(type);
    }

    public static DUUIDeserializer<DUUIEvent> remoteDeserializer() {
        return input -> {
            String value = new String(input.readAllBytes(), StandardCharsets.UTF_8).trim();
            if (value.startsWith("data:")) {
                value = value.substring("data:".length()).trim();
            }
            return fromRemotePayload(value);
        };
    }

    private static DUUIEvent fromRemotePayload(String json) throws Exception {
        Map<String, Object> values = JSON.readValue(json, new TypeReference<>() {});
        String type = string(values.get("type"));
        if (type == null) {
            type = string(values.get("event"));
        }
        if (type == null && values.containsKey("stream_id")) {
            type = "handshake";
        }
        DUUIEventContext context = context(values);
        if ("metric".equalsIgnoreCase(type)) {
            Object dataPoints = values.get("data_points");
            Map<String, Object> point = dataPoints instanceof List<?> list && !list.isEmpty()
                    ? map(list.get(0))
                    : Map.of();
            Map<String, String> tags = stringMap(values.get("tags"));
            tags.putAll(stringMap(values.get("attributes")));
            tags.putAll(stringMap(point.get("attributes")));
            Double value = number(values.get("value"));
            if (value == null) {
                for (String key : List.of("as_double", "count", "sum", "value")) {
                    value = number(point.get(key));
                    if (value != null) {
                        break;
                    }
                }
            }
            return DUUIEvent.builder(DUUIEventType.METRIC)
                    .context(context)
                    .name(firstPresent(attribute(values, "duui.metric.category"), string(values.get("metric_type")), "remote-metric"))
                    .metric(string(values.get("name")), value, string(values.get("unit")))
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

    private static String attribute(Map<String, Object> values, String key) {
        return string(map(values.get("attributes")).get(key));
    }

    private static String attribute(Map<String, Object> attributes, Map<String, Object> resource, String key) {
        return firstPresent(string(attributes.get(key)), string(resource.get(key)));
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

    private static String firstPresent(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    public String id() { return id; }
    public Instant timestamp() { return timestamp; }
    public DUUIEventType type() { return type; }
    public DUUIEventLevel level() { return level; }
    public DUUIEventStatus status() { return status; }
    public DUUIEventContext context() { return context; }
    public String traceId() { return context.trace().traceId(); }
    public String spanId() { return context.trace().spanId(); }
    public String parentSpanId() { return context.trace().parentSpanId(); }
    public String orchestratorId() { return context.orchestratorId(); }
    public String taskId() { return context.taskId(); }
    public String artifactId() { return context.artifactId(); }
    public String checkpointId() { return context.checkpointId(); }
    public String stageId() { return context.stageId(); }
    public String componentId() { return context.componentId(); }
    public String nodeId() { return context.nodeId(); }
    public String annotatorId() { return context.annotatorId(); }
    public String workerId() { return context.workerId(); }
    public String name() { return name; }
    public String message() { return message; }
    public String metricName() { return metricName; }
    public Double metricValue() { return metricValue; }
    public String metricUnit() { return metricUnit; }
    public long metricIntervalMs() { return metricIntervalMs; }
    public Map<String, String> metricTags() { return metricTags; }
    public String errorType() { return errorType; }
    public String stackTrace() { return stackTrace; }
    public String recoveryHint() { return recoveryHint; }
    public Map<String, Object> attributes() { return attributes; }

    public Builder toBuilder() {
        return builder(type)
                .id(id)
                .timestamp(timestamp)
                .level(level)
                .status(status)
                .context(context)
                .name(name)
                .message(message)
                .metric(metricName, metricValue, metricUnit, metricIntervalMs)
                .metricTags(metricTags)
                .error(errorType, stackTrace, recoveryHint)
                .attributes(attributes);
    }

    public static final class Builder {
        private String id;
        private Instant timestamp;
        private final DUUIEventType type;
        private DUUIEventLevel level;
        private DUUIEventStatus status;
        private DUUIEventContext context;
        private String name;
        private String message;
        private String metricName;
        private Double metricValue;
        private String metricUnit;
        private long metricIntervalMs;
        private final Map<String, String> metricTags = new LinkedHashMap<>();
        private String errorType;
        private String stackTrace;
        private String recoveryHint;
        private final Map<String, Object> attributes = new LinkedHashMap<>();

        private Builder(DUUIEventType type) {
            this.type = Objects.requireNonNull(type, "type");
        }

        public Builder id(String id) { this.id = id; return this; }
        public Builder timestamp(Instant timestamp) { this.timestamp = timestamp; return this; }
        public Builder level(DUUIEventLevel level) { this.level = level; return this; }
        public Builder status(DUUIEventStatus status) { this.status = status; return this; }
        public Builder context(DUUIEventContext context) { this.context = context; return this; }
        public Builder name(String name) { this.name = name; return this; }
        public Builder message(String message) { this.message = message; return this; }
        public Builder metric(String name, Double value, String unit) {
            return metric(name, value, unit, 0L);
        }
        public Builder metric(String name, Double value, String unit, long intervalMs) {
            this.metricName = name;
            this.metricValue = value;
            this.metricUnit = unit;
            this.metricIntervalMs = Math.max(0L, intervalMs);
            return this;
        }
        public Builder metricTags(Map<String, String> tags) {
            if (tags != null) this.metricTags.putAll(tags);
            return this;
        }
        public Builder error(String type, String stackTrace, String recoveryHint) {
            this.errorType = type;
            this.stackTrace = stackTrace;
            this.recoveryHint = recoveryHint;
            return this;
        }
        public Builder attribute(String key, Object value) {
            if (key != null && value != null) this.attributes.put(key, value);
            return this;
        }
        public Builder attributes(Map<String, ?> values) {
            if (values != null) values.forEach(this::attribute);
            return this;
        }

        public DUUIEvent build() {
            return new DUUIEvent(this);
        }
    }
}

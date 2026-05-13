package org.texttechnologylab.duui.event;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DUUIEvent {
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
        this.metricTags = Map.copyOf(builder.metricTags);
        this.errorType = builder.errorType;
        this.stackTrace = builder.stackTrace;
        this.recoveryHint = builder.recoveryHint;
        this.attributes = Map.copyOf(builder.attributes);
    }

    public static Builder builder(DUUIEventType type) {
        return new Builder(type);
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
    public Map<String, String> metricTags() { return metricTags; }
    public String errorType() { return errorType; }
    public String stackTrace() { return stackTrace; }
    public String recoveryHint() { return recoveryHint; }
    public Map<String, Object> attributes() { return attributes; }

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
            this.metricName = name;
            this.metricValue = value;
            this.metricUnit = unit;
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

package org.texttechnologylab.duui.refactor.events;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public final class DUUIMetric extends DUUIEvent {
    private final String category;
    private final String name;
    private final double value;
    private final String unit;
    private final Duration interval;
    private final Map<String, String> tags;

    public DUUIMetric(String category, String name, double value, String unit, Duration interval, Map<String, String> tags) {
        this(category, name, value, unit, interval, tags, DUUIEventContext.current());
    }

    public DUUIMetric(String category, String name, double value, String unit, Duration interval, Map<String, String> tags, DUUIEventContext context) {
        super(DUUIEventType.METRIC, context);
        this.category = Objects.requireNonNull(category, "category");
        this.name = Objects.requireNonNull(name, "name");
        this.value = value;
        this.unit = Objects.requireNonNull(unit, "unit");
        this.interval = interval == null ? Duration.ZERO : interval;
        this.tags = Map.copyOf(tags == null ? Map.of() : tags);
    }

    public String category() {
        return category;
    }

    public String name() {
        return name;
    }

    public double value() {
        return value;
    }

    public String unit() {
        return unit;
    }

    public Duration interval() {
        return interval;
    }

    public Map<String, String> tags() {
        return tags;
    }
}

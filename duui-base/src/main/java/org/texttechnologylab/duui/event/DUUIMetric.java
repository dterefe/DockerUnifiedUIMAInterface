package org.texttechnologylab.duui.event;

import java.util.Map;

public record DUUIMetric(
        String category,
        String name,
        double value,
        String unit,
        long intervalMs,
        Map<String, String> tags
) {
    public DUUIMetric {
        tags = Map.copyOf(tags == null ? Map.of() : tags);
    }

    public DUUIEvent event(DUUIEventContext context) {
        return DUUIEvent.builder(DUUIEventType.METRIC)
                .context(context)
                .name(category)
                .metric(name, value, unit, intervalMs)
                .metricTags(tags)
                .build();
    }
}

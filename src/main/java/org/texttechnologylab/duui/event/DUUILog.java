package org.texttechnologylab.duui.event;

import java.util.Map;

public record DUUILog(
        DUUIEventLevel level,
        String message,
        Map<String, Object> extra
) {
    public DUUILog {
        extra = Map.copyOf(extra == null ? Map.of() : extra);
    }

    public DUUIEvent event(String name, DUUIEventContext context) {
        return DUUIEvent.builder(DUUIEventType.LOG)
                .context(context)
                .name(name)
                .level(level)
                .message(message)
                .attributes(extra)
                .build();
    }
}

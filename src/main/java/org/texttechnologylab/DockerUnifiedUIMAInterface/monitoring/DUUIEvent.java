package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;

public class DUUIEvent {

    private static final java.util.concurrent.atomic.AtomicLong LAST_ID = new java.util.concurrent.atomic.AtomicLong(0);

    public enum Sender {
        SYSTEM,
        COMPOSER,
        DRIVER,
        COMPONENT,
        HANDLER,
        DOCUMENT,
        STORAGE,
        READER,
        WRITER,
        WORKER,
        MONITOR;
    }

    private final String className;
    private final Sender sender;
    private final String message;
    private final long timestamp;
    private final long id;
    private final DUUIComposer.DebugLevel debugLevel;
    private final DUUIContext context;

    public DUUIEvent(Sender sender, String message) {
        this(sender, message, Instant.now().toEpochMilli(), DUUIComposer.DebugLevel.NONE, DUUIContexts.defaultContext(), "");
    }

    public DUUIEvent(Sender sender, String message, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, Instant.now().toEpochMilli(), debugLevel, DUUIContexts.defaultContext(), "");
    }

    public DUUIEvent(Sender sender, String message, long timestamp) {
        this(sender, message, timestamp, DUUIComposer.DebugLevel.NONE, DUUIContexts.defaultContext(), "");
    }

    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, timestamp, debugLevel, DUUIContexts.defaultContext(), "");
    }

    public DUUIEvent(Sender sender, String message, DebugLevel debugLevel, DUUIContext context, String name) {
        this(sender, message, Instant.now().toEpochMilli(), debugLevel, context, name);
    }

    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel, DUUIContext context, String name) {
        this.sender = Objects.requireNonNullElse(sender, Sender.SYSTEM);
        this.message = StringUtils.stripEnd(StringUtils.defaultString(message), "\n\r") + "\n";
        this.timestamp = timestamp;
        this.id = nextIdForTimestamp(timestamp);
        this.debugLevel = Objects.requireNonNullElse(debugLevel, DebugLevel.NONE);
        this.context = context != null ? context : DUUIContexts.defaultContext();
        this.className = StringUtils.defaultString(name);
    }

    private static long nextIdForTimestamp(long timestamp) {
        long ts = Math.max(0L, timestamp);
        while (true) {
            long prev = LAST_ID.get();
            long next = Math.max(prev + 1L, ts);
            if (LAST_ID.compareAndSet(prev, next)) {
                return next;
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DUUIEvent other)) return false;
        return id == other.id;
    }

    @Override
    public String toString() {
        if (isEmpty()) return "";

        String ts = String.valueOf(timestamp);
        if (DUUILoggingConfig.isFormatTimestamp()) {
            ts = Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

        String levelPart = debugLevel.equals(DebugLevel.NONE) ? "" : debugLevel.toString();
        String senderPart = DUUILoggingConfig.isIncludeSender() ? sender.name() : "";
        String classPart = DUUILoggingConfig.isIncludeClassName() && className != null && !className.isEmpty()
                ? className
                : "";

        String base = String.format("%s %s %s %s %s",
                ts,
                levelPart,
                senderPart,
                classPart,
                message
        );

        // if (DUUILoggingConfig.isIncludePayload() && context != null && context.hasPayload()) {
        //     String logs = context.payload();
        //     if (StringUtils.isNotEmpty(logs)) {
        //         String indented = "    " + logs.replace("\n", "\n    ");
        //         indented = StringUtils.stripEnd(indented, "\n\r");
        //         return base + "\n" + indented + "\n";
        //     }
        // }

        return base;
    }

    public long getId() {
        return id;
    }

    public Document toDocument() {
        return new Document("event_id", id)
            .append("timestamp", timestamp)
            .append("status", context.status().value())
            .append("thread", context.thread())
            .append("sender", sender.name())
            .append("className", className)
            .append("debugLevel", debugLevel.name())
            .append("message", message)
            .append("payload", DUUIContexts.toDocument(context.payloadRecord()))
            .append("context", DUUIContexts.toDocument(context));
    }

    public Sender getSender() {
        return sender;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DUUIComposer.DebugLevel getDebugLevel() {
        return debugLevel;
    }

    public DUUIContext getContext() {
        return context;
    }

    public boolean isEmpty() {
        return !StringUtils.isNotBlank(message);
    }
}

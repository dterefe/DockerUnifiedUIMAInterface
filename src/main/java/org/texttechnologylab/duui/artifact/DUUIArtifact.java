package org.texttechnologylab.duui.artifact;

import org.texttechnologylab.duui.exception.DUUIFailureLog;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class DUUIArtifact<T> {
    private final String id;
    private final T payload;
    private final Class<T> payloadType;
    private final DUUIArtifactType<T> artifactType;
    private final DUUIArtifactContext context;
    private final DUUIArtifactState state;
    private final DUUIStats stats;
    private final DUUIFailureLog failures;
    private final DUUIMetadata metadata;

    private DUUIArtifact(
            String id,
            T payload,
            Class<T> payloadType,
            DUUIArtifactType<T> artifactType,
            DUUIArtifactContext context,
            DUUIArtifactState state,
            DUUIStats stats,
            DUUIFailureLog failures,
            DUUIMetadata metadata
    ) {
        this.id = Objects.requireNonNull(id, "id");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.payloadType = payloadType;
        this.artifactType = artifactType == null
                ? DUUIArtifactType.javaType(Objects.requireNonNull(payloadType, "payloadType"))
                : artifactType;
        this.context = context == null ? DUUIArtifactContext.empty() : context;
        this.state = state == null ? new DUUIArtifactState() : state;
        this.stats = stats == null ? new DUUIStats() : stats;
        this.failures = failures == null ? new DUUIFailureLog() : failures;
        this.metadata = metadata == null ? new DUUIMetadata() : metadata;
    }

    public static <T> DUUIArtifact<T> of(T payload, Class<T> payloadType) {
        return new DUUIArtifact<>(
                UUID.randomUUID().toString(),
                payload,
                payloadType,
                DUUIArtifactType.javaType(payloadType),
                DUUIArtifactContext.empty(),
                new DUUIArtifactState(),
                new DUUIStats(),
                new DUUIFailureLog(),
                new DUUIMetadata()
        );
    }

    public static <T> DUUIArtifact<T> of(T payload, DUUIArtifactType<T> artifactType) {
        return new DUUIArtifact<>(
                UUID.randomUUID().toString(),
                payload,
                null,
                artifactType,
                DUUIArtifactContext.empty(),
                new DUUIArtifactState(),
                new DUUIStats(),
                new DUUIFailureLog(),
                new DUUIMetadata()
        );
    }

    public DUUIArtifact<T> withContext(DUUIArtifactContext context) {
        return new DUUIArtifact<>(id, payload, payloadType, artifactType, context, state, stats, failures, metadata);
    }

    public DUUIArtifact<T> childContext(String parentArtifactId) {
        return withContext(context.toBuilder().parentArtifactId(parentArtifactId).createdAt(Instant.now()).build());
    }

    public DUUIArtifact<T> successorContext(String predecessorArtifactId) {
        return withContext(context.toBuilder().predecessorArtifactId(predecessorArtifactId).createdAt(Instant.now()).build());
    }

    public <U> DUUIArtifact<U> childArtifact(U payload, Class<U> payloadType) {
        return DUUIArtifact.of(payload, payloadType).withContext(context.toBuilder()
                .parentArtifactId(id)
                .sourceArtifactId(context.sourceArtifactId() == null ? id : context.sourceArtifactId())
                .createdAt(Instant.now())
                .build());
    }

    public <U> DUUIArtifact<U> childArtifact(U payload, DUUIArtifactType<U> artifactType) {
        return DUUIArtifact.of(payload, artifactType).withContext(context.toBuilder()
                .parentArtifactId(id)
                .sourceArtifactId(context.sourceArtifactId() == null ? id : context.sourceArtifactId())
                .createdAt(Instant.now())
                .build());
    }

    public <U> DUUIArtifact<U> successorArtifact(U payload, Class<U> payloadType) {
        return DUUIArtifact.of(payload, payloadType).withContext(context.toBuilder()
                .predecessorArtifactId(id)
                .sourceArtifactId(context.sourceArtifactId() == null ? id : context.sourceArtifactId())
                .createdAt(Instant.now())
                .build());
    }

    public <U> DUUIArtifact<U> successorArtifact(U payload, DUUIArtifactType<U> artifactType) {
        return DUUIArtifact.of(payload, artifactType).withContext(context.toBuilder()
                .predecessorArtifactId(id)
                .sourceArtifactId(context.sourceArtifactId() == null ? id : context.sourceArtifactId())
                .createdAt(Instant.now())
                .build());
    }

    public String id() { return id; }
    public T payload() { return payload; }
    public Class<T> payloadType() { return payloadType; }
    public DUUIArtifactType<T> artifactType() { return artifactType; }
    public DUUIArtifactContext context() { return context; }
    public DUUIArtifactState state() { return state; }
    public DUUIStats stats() { return stats; }
    public DUUIFailureLog failures() { return failures; }
    public DUUIMetadata metadata() { return metadata; }
}

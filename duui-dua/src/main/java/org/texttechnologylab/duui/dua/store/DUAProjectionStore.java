package org.texttechnologylab.duui.dua.store;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import org.texttechnologylab.duui.dua.projection.DUAProjection;
import org.texttechnologylab.duui.dua.projection.DUAProjectionType;

public final class DUAProjectionStore {
    private final ConcurrentMap<String, DUAProjection<?>> byTypeName = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, DUAProjection<?>> byMarkerClass = new ConcurrentHashMap<>();

    public <T> DUAProjection<T> register(DUAProjectionType<T> type) {
        Objects.requireNonNull(type, "type");
        DUAProjection<?> existingByType = byTypeName.get(type.typeName());
        DUAProjection<?> existingByMarker = byMarkerClass.get(type.markerClass());
        if (existingByType != null) {
            ensureCompatible(type, existingByType.type());
            return cast(existingByType);
        }
        if (existingByMarker != null) {
            ensureCompatible(type, existingByMarker.type());
            return cast(existingByMarker);
        }
        DUAProjection<T> projection = DUAProjection.create(type);
        existingByType = byTypeName.putIfAbsent(type.typeName(), projection);
        if (existingByType != null) {
            ensureCompatible(type, existingByType.type());
            return cast(existingByType);
        }
        existingByMarker = byMarkerClass.putIfAbsent(type.markerClass(), projection);
        if (existingByMarker != null) {
            byTypeName.remove(type.typeName(), projection);
            ensureCompatible(type, existingByMarker.type());
            return cast(existingByMarker);
        }
        return projection;
    }

    public <T> Optional<DUAProjection<T>> find(DUAProjectionType<T> type) {
        Objects.requireNonNull(type, "type");
        return find(type.typeName()).map(projection -> {
            ensureCompatible(type, projection.type());
            return cast(projection);
        });
    }

    public Optional<DUAProjection<?>> find(String typeName) {
        Objects.requireNonNull(typeName, "typeName");
        return Optional.ofNullable(byTypeName.get(typeName));
    }

    public <T> Optional<DUAProjection<T>> find(Class<T> markerClass) {
        Objects.requireNonNull(markerClass, "markerClass");
        return Optional.ofNullable(byMarkerClass.get(markerClass)).map(this::cast);
    }

    public Stream<DUAProjection<?>> stream() {
        return byTypeName.values().stream();
    }

    private static void ensureCompatible(DUAProjectionType<?> requested, DUAProjectionType<?> existing) {
        if (!requested.typeName().equals(existing.typeName())
                || !requested.markerClass().equals(existing.markerClass())) {
            throw new DUAStoreException("Projection type collision for " + requested.modeName()
                    + " and " + existing.modeName());
        }
    }

    @SuppressWarnings("unchecked")
    private <T> DUAProjection<T> cast(DUAProjection<?> projection) {
        return (DUAProjection<T>) projection;
    }
}

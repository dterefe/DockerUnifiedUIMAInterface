package org.texttechnologylab.duui.dua.store.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUAValueQuery;
import org.texttechnologylab.duui.dua.store.DUAValueQueryStore;
import org.texttechnologylab.duui.dua.store.DUAValueRow;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValueKind;

/**
 * In-memory implementation of {@link DUAValueQueryStore}.
 * <p>
 * Provides full FS lifecycle management with concurrent-safe indexes:
 * <ul>
 *   <li>Primary FS storage indexed by fsRef</li>
 *   <li>Feature→value inverted index for fast equality lookups</li>
 *   <li>Type→FS index for type-based queries</li>
 *   <li>Reverse reference index for reference traversal</li>
 * </ul>
 */
public final class DUAMemoryValueQueryStore implements DUAValueQueryStore {

    private final AtomicLong nextFsRef = new AtomicLong(1);
    private final ConcurrentHashMap<Long, FsRecord> fsRecords = new ConcurrentHashMap<>();

    // featureCode -> value -> Set<fsRef>
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Object, CopyOnWriteArraySet<Long>>>
            featureValueIndex = new ConcurrentHashMap<>();

    // typeCode -> Set<fsRef>
    private final ConcurrentHashMap<Integer, CopyOnWriteArraySet<Long>> typeIndex = new ConcurrentHashMap<>();

    // featureCode -> targetFsRef -> Set<sourceFsRef>
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, CopyOnWriteArraySet<Long>>>
            reverseRefIndex = new ConcurrentHashMap<>();

    // ========== FS Lifecycle ==========

    /**
     * Creates a new feature structure with the given type code.
     *
     * @param typeCode the type code
     * @return the newly allocated fsRef
     */
    public long createFS(int typeCode) {
        if (typeCode < 0) {
            throw new IllegalArgumentException("typeCode must not be negative");
        }
        long fsRef = nextFsRef.getAndIncrement();
        fsRecords.put(fsRef, new FsRecord(typeCode, new ConcurrentHashMap<>(), false));
        typeIndex.computeIfAbsent(typeCode, k -> new CopyOnWriteArraySet<>()).add(fsRef);
        return fsRef;
    }

    /**
     * Sets a feature value on the specified FS.
     *
     * @param fsRef       the feature structure reference
     * @param featureCode the feature code
     * @param value       the value to set
     */
    public void setFeature(long fsRef, int featureCode, Object value) {
        FsRecord record = fsRecords.get(fsRef);
        if (record == null) {
            throw new IllegalArgumentException("FS not found: " + fsRef);
        }
        if (record.deleted()) {
            throw new IllegalStateException("FS is deleted: " + fsRef);
        }

        // Remove old value from inverted index
        Object oldValue = record.features().put(featureCode, value);
        if (oldValue != null) {
            removeFromFeatureIndex(fsRef, featureCode, oldValue);
        }

        // Add new value to inverted index
        if (value != null) {
            featureValueIndex
                    .computeIfAbsent(featureCode, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(value, k -> new CopyOnWriteArraySet<>())
                    .add(fsRef);
        }

        // If the value is a reference (long), update reverse reference index
        if (value instanceof Number num) {
            long targetRef = num.longValue();
            if (oldValue instanceof Number oldNum) {
                removeFromReverseRefIndex(fsRef, featureCode, oldNum.longValue());
            }
            reverseRefIndex
                    .computeIfAbsent(featureCode, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(targetRef, k -> new CopyOnWriteArraySet<>())
                    .add(fsRef);
        } else if (oldValue instanceof Number oldNum) {
            removeFromReverseRefIndex(fsRef, featureCode, oldNum.longValue());
        }
    }

    /**
     * Gets a feature value from the specified FS.
     *
     * @param fsRef       the feature structure reference
     * @param featureCode the feature code
     * @return the value, or {@code null} if not set
     */
    public Object getFeature(long fsRef, int featureCode) {
        FsRecord record = fsRecords.get(fsRef);
        if (record == null) {
            return null;
        }
        return record.features().get(featureCode);
    }

    /**
     * Returns an unmodifiable view of all features on the specified FS.
     *
     * @param fsRef the feature structure reference
     * @return map of feature code to value
     */
    public Map<Integer, Object> getFeatures(long fsRef) {
        FsRecord record = fsRecords.get(fsRef);
        if (record == null) {
            return Map.of();
        }
        return Collections.unmodifiableMap(record.features());
    }

    /**
     * Deletes the specified FS from all indexes.
     *
     * @param fsRef the feature structure reference
     */
    public void deleteFS(long fsRef) {
        FsRecord record = fsRecords.remove(fsRef);
        if (record == null) {
            return;
        }
        // Remove from type index
        CopyOnWriteArraySet<Long> typeSet = typeIndex.get(record.typeCode());
        if (typeSet != null) {
            typeSet.remove(fsRef);
        }
        // Remove from feature value index
        for (Map.Entry<Integer, Object> entry : record.features().entrySet()) {
            removeFromFeatureIndex(fsRef, entry.getKey(), entry.getValue());
            if (entry.getValue() instanceof Number num) {
                removeFromReverseRefIndex(fsRef, entry.getKey(), num.longValue());
            }
        }
    }

    /**
     * Marks the FS as deleted without removing it from indexes.
     *
     * @param fsRef the feature structure reference
     */
    public void markDeleted(long fsRef) {
        FsRecord record = fsRecords.get(fsRef);
        if (record != null) {
            fsRecords.put(fsRef, new FsRecord(record.typeCode(), record.features(), true));
        }
    }

    /**
     * Checks if an FS exists and is not marked deleted.
     *
     * @param fsRef the feature structure reference
     * @return {@code true} if the FS exists and is not deleted
     */
    public boolean exists(long fsRef) {
        FsRecord record = fsRecords.get(fsRef);
        return record != null && !record.deleted();
    }

    /**
     * Returns the type code for the given FS.
     *
     * @param fsRef the feature structure reference
     * @return the type code, or -1 if not found
     */
    public int getTypeCode(long fsRef) {
        FsRecord record = fsRecords.get(fsRef);
        return record == null ? -1 : record.typeCode();
    }

    /**
     * Creates multiple FS instances of the same type.
     *
     * @param typeCode the type code
     * @param count    the number of FS instances to create
     * @return list of newly allocated fsRefs
     */
    public List<Long> bulkCreateFS(int typeCode, int count) {
        if (typeCode < 0) {
            throw new IllegalArgumentException("typeCode must not be negative");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        List<Long> refs = new ArrayList<>(count);
        long base = nextFsRef.getAndAdd(count);
        CopyOnWriteArraySet<Long> typeSet = typeIndex.computeIfAbsent(typeCode, k -> new CopyOnWriteArraySet<>());
        for (int i = 0; i < count; i++) {
            long fsRef = base + i;
            fsRecords.put(fsRef, new FsRecord(typeCode, new ConcurrentHashMap<>(), false));
            typeSet.add(fsRef);
            refs.add(fsRef);
        }
        return refs;
    }

    // ========== Query ==========

    @Override
    public Stream<DUAValueRow> find(DUAValueQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUAValueQuery.FeatureEquals q -> findByFeatureEquals(q);
            case DUAValueQuery.FeatureRange q -> findByFeatureRange(q);
            case DUAValueQuery.ReferenceTarget q -> findByReferenceTarget(q);
            case DUAValueQuery.CollectionContains q -> findByCollectionContains(q);
        };
    }

    private Stream<DUAValueRow> findByFeatureEquals(DUAValueQuery.FeatureEquals query) {
        int featureCode = query.featureName().hashCode();
        Object searchValue = query.value().value();

        // Use inverted index for exact match
        ConcurrentHashMap<Object, CopyOnWriteArraySet<Long>> valueIndex =
                featureValueIndex.get(featureCode);
        if (valueIndex == null) {
            return Stream.empty();
        }
        CopyOnWriteArraySet<Long> matches = valueIndex.get(searchValue);
        if (matches == null) {
            return Stream.empty();
        }

        Stream<Long> refs = matches.stream()
                .filter(this::exists);

        // Apply type filter if present
        if (query.typeId().isPresent()) {
            int typeId = query.typeId().getAsInt();
            refs = refs.filter(fsRef -> getTypeCode(fsRef) == typeId);
        }

        return refs.map(fsRef -> toRow(query.casId(), query.viewId(), fsRef, featureCode, query.featureName()));
    }

    private Stream<DUAValueRow> findByFeatureRange(DUAValueQuery.FeatureRange query) {
        int featureCode = query.featureName().hashCode();
        ConcurrentHashMap<Object, CopyOnWriteArraySet<Long>> valueIndex =
                featureValueIndex.get(featureCode);
        if (valueIndex == null) {
            return Stream.empty();
        }

        long lower = query.lowerInclusive();
        long upper = query.upperInclusive();

        return valueIndex.entrySet().stream()
                .filter(entry -> {
                    Object key = entry.getKey();
                    if (key instanceof Number num) {
                        long val = num.longValue();
                        return val >= lower && val <= upper;
                    }
                    return false;
                })
                .flatMap(entry -> entry.getValue().stream())
                .filter(this::exists)
                .filter(fsRef -> query.typeId().isEmpty() || getTypeCode(fsRef) == query.typeId().getAsInt())
                .map(fsRef -> toRow(query.casId(), query.viewId(), fsRef, featureCode, query.featureName()));
    }

    private Stream<DUAValueRow> findByReferenceTarget(DUAValueQuery.ReferenceTarget query) {
        int featureCode = query.featureName().hashCode();
        long targetFsRef = query.targetFsRef();

        // Use reverse reference index
        ConcurrentHashMap<Long, CopyOnWriteArraySet<Long>> refIndex =
                reverseRefIndex.get(featureCode);
        if (refIndex == null) {
            return Stream.empty();
        }
        CopyOnWriteArraySet<Long> sources = refIndex.get(targetFsRef);
        if (sources == null) {
            return Stream.empty();
        }

        return sources.stream()
                .filter(this::exists)
                .map(fsRef -> toRow(query.casId(), query.viewId(), fsRef, featureCode, query.featureName()));
    }

    private Stream<DUAValueRow> findByCollectionContains(DUAValueQuery.CollectionContains query) {
        long collectionFsRef = query.collectionFsRef();
        DUACasValue searchValue = query.value();

        FsRecord record = fsRecords.get(collectionFsRef);
        if (record == null || record.deleted()) {
            return Stream.empty();
        }

        // Check all features of the collection FS for array-like values
        return record.features().entrySet().stream()
                .filter(entry -> {
                    Object val = entry.getValue();
                    if (val instanceof List<?> list) {
                        return list.contains(searchValue.value());
                    }
                    return searchValue.value().equals(val);
                })
                .map(entry -> toRow(query.casId(), query.viewId(), collectionFsRef,
                        entry.getKey(), String.valueOf(entry.getKey())));
    }

    // ========== Internal Helpers ==========

    private void removeFromFeatureIndex(long fsRef, int featureCode, Object value) {
        if (value == null) return;
        ConcurrentHashMap<Object, CopyOnWriteArraySet<Long>> valueIndex =
                featureValueIndex.get(featureCode);
        if (valueIndex != null) {
            CopyOnWriteArraySet<Long> refs = valueIndex.get(value);
            if (refs != null) {
                refs.remove(fsRef);
                if (refs.isEmpty()) {
                    valueIndex.remove(value);
                }
            }
        }
    }

    private void removeFromReverseRefIndex(long fsRef, int featureCode, long targetRef) {
        ConcurrentHashMap<Long, CopyOnWriteArraySet<Long>> refIndex =
                reverseRefIndex.get(featureCode);
        if (refIndex != null) {
            CopyOnWriteArraySet<Long> sources = refIndex.get(targetRef);
            if (sources != null) {
                sources.remove(fsRef);
                if (sources.isEmpty()) {
                    refIndex.remove(targetRef);
                }
            }
        }
    }

    private DUAValueRow toRow(DUAId casId, DUAId viewId, long fsRef, int featureCode, String featureName) {
        FsRecord record = fsRecords.get(fsRef);
        Object rawValue = record == null ? null : record.features().get(featureCode);
        DUACasValue value;
        if (rawValue instanceof DUACasValue cv) {
            value = cv;
        } else if (rawValue instanceof String s) {
            value = DUACasValue.of(s);
        } else if (rawValue instanceof Boolean b) {
            value = DUACasValue.of(b);
        } else if (rawValue instanceof Number n) {
            value = DUACasValue.ofLong(n.longValue());
        } else if (rawValue == null) {
            value = new DUACasValue(DUACasValueKind.STRING, null);
        } else {
            value = DUACasValue.of(rawValue.toString());
        }
        return new DUAValueRow(casId, viewId, fsRef, featureCode, featureName, value);
    }

    /**
     * Internal record for storing FS metadata and features.
     */
    private record FsRecord(int typeCode, ConcurrentHashMap<Integer, Object> features, boolean deleted) {
    }
}

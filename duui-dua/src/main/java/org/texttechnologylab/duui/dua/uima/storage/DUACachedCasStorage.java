package org.texttechnologylab.duui.dua.uima.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class DUACachedCasStorage implements DUAFastCasStorage {
    private final DUACasStorage delegate;
    private final Lru<SlotKey, Optional<DUACasValue>> slots;
    private final Lru<SlotKey, Integer> intSlots;
    private final Lru<ArrayKey, Integer> arraySizes;
    private final Lru<ArrayValueKey, Optional<DUACasValue>> arrayValues;
    private final Lru<Integer, String> stringsByCode;
    private final Lru<String, Integer> stringsByValue;

    public DUACachedCasStorage(DUACasStorage delegate, int maxEntriesPerCache) {
        if (maxEntriesPerCache < 1) {
            throw new IllegalArgumentException("maxEntriesPerCache must be positive");
        }
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.slots = new Lru<>(maxEntriesPerCache);
        this.intSlots = new Lru<>(maxEntriesPerCache);
        this.arraySizes = new Lru<>(maxEntriesPerCache);
        this.arrayValues = new Lru<>(maxEntriesPerCache);
        this.stringsByCode = new Lru<>(maxEntriesPerCache);
        this.stringsByValue = new Lru<>(maxEntriesPerCache);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        return readSlot(fsRef, featureName.hashCode(), featureName);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        SlotKey key = new SlotKey(fsRef, featureCode);
        Optional<Optional<DUACasValue>> cached = slots.getCached(key);
        if (cached.isPresent()) {
            return cached.orElseThrow();
        }
        Optional<DUACasValue> value = delegate.readSlot(fsRef, featureCode, featureName);
        slots.putCached(key, value);
        return value;
    }

    @Override
    public void writeSlot(int fsRef, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName.hashCode(), featureName, value);
    }

    @Override
    public void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        delegate.writeSlot(fsRef, featureCode, featureName, value);
        if (value.kind() == DUACasValueKind.INTEGER && value.value() != null) {
            intSlots.putCached(new SlotKey(fsRef, featureCode), value.intValue());
        }
        slots.putCached(new SlotKey(fsRef, featureCode), value.value() == null ? Optional.empty() : Optional.of(value));
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        SlotKey key = new SlotKey(fsRef, featureCode);
        Optional<Integer> cachedInt = intSlots.getCached(key);
        if (cachedInt.isPresent()) {
            return cachedInt.orElseThrow();
        }
        Optional<Optional<DUACasValue>> cached = slots.getCached(key);
        if (cached.isPresent()) {
            return cached.orElseThrow().map(DUACasValue::intValue).orElse(defaultValue);
        }
        int value = delegate instanceof DUAFastCasStorage fast
                ? fast.readIntSlotOrDefault(fsRef, featureCode, featureName, defaultValue)
                : delegate.readSlot(fsRef, featureCode, featureName).map(DUACasValue::intValue).orElse(defaultValue);
        intSlots.putCached(key, value);
        return value;
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        if (delegate instanceof DUAFastCasStorage fast) {
            fast.writeIntSlot(fsRef, featureCode, featureName, value);
        } else {
            delegate.writeSlot(fsRef, featureCode, featureName, DUACasValue.ofInt(value));
        }
        intSlots.putCached(new SlotKey(fsRef, featureCode), value);
    }

    @Override
    public void initializeArray(DUACasArrayKind kind, int fsRef, int length) {
        delegate.initializeArray(kind, fsRef, length);
        ArrayKey key = new ArrayKey(kind, fsRef);
        Integer current = arraySizes.getCached(key).orElse(0);
        arraySizes.putCached(key, Math.max(current, length));
    }

    @Override
    public int arraySize(DUACasArrayKind kind, int fsRef) {
        ArrayKey key = new ArrayKey(kind, fsRef);
        Optional<Integer> cached = arraySizes.getCached(key);
        if (cached.isPresent()) {
            return cached.orElseThrow();
        }
        int size = delegate.arraySize(kind, fsRef);
        arraySizes.putCached(key, size);
        return size;
    }

    @Override
    public Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        ArrayValueKey key = new ArrayValueKey(kind, fsRef, index);
        Optional<Optional<DUACasValue>> cached = arrayValues.getCached(key);
        if (cached.isPresent()) {
            return cached.orElseThrow();
        }
        Optional<DUACasValue> value = delegate.readArrayValue(kind, fsRef, index);
        arrayValues.putCached(key, value);
        return value;
    }

    @Override
    public void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value) {
        delegate.writeArrayValue(kind, fsRef, index, value);
        arrayValues.putCached(new ArrayValueKey(kind, fsRef, index),
                value.value() == null ? Optional.empty() : Optional.of(value));
        ArrayKey arrayKey = new ArrayKey(kind, fsRef);
        Integer current = arraySizes.getCached(arrayKey).orElse(0);
        arraySizes.putCached(arrayKey, Math.max(current, index + 1));
    }

    @Override
    public String stringForCode(int code) {
        Optional<String> cached = stringsByCode.getCached(code);
        if (cached.isPresent()) {
            return cached.orElseThrow();
        }
        String value = delegate.stringForCode(code);
        if (value != null) {
            stringsByCode.putCached(code, value);
            stringsByValue.putCached(value, code);
        }
        return value;
    }

    @Override
    public int codeForString(String value) {
        Optional<Integer> cached = stringsByValue.getCached(value);
        if (cached.isPresent()) {
            return cached.orElseThrow();
        }
        int code = delegate.codeForString(value);
        if (value != null) {
            stringsByValue.putCached(value, code);
            stringsByCode.putCached(code, value);
        }
        return code;
    }

    @Override
    public int allocateFsId(int typeCode, int viewId) {
        return delegate.allocateFsId(typeCode, viewId);
    }

    @Override
    public void onFsCreated(int fsRef, int typeCode, int viewId) {
        delegate.onFsCreated(fsRef, typeCode, viewId);
    }

    @Override
    public void onFsDeleted(int fsRef) {
        delegate.onFsDeleted(fsRef);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private record SlotKey(int fsRef, int featureCode) {
    }

    private record ArrayKey(DUACasArrayKind kind, int fsRef) {
    }

    private record ArrayValueKey(DUACasArrayKind kind, int fsRef, int index) {
    }

    private static final class Lru<K, V> {
        private final int maxEntries;
        private final LinkedHashMap<K, V> values;

        private Lru(int maxEntries) {
            this.maxEntries = maxEntries;
            this.values = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    return size() > Lru.this.maxEntries;
                }
            };
        }

        private synchronized Optional<V> getCached(K key) {
            return values.containsKey(key) ? Optional.ofNullable(values.get(key)) : Optional.empty();
        }

        private synchronized void putCached(K key, V value) {
            values.put(key, value);
        }
    }
}

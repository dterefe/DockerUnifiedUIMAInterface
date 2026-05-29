package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class DUAConcurrentMemoryCasStorage implements DUAFastCasStorage {
    private final ConcurrentHashMap<SlotKey, DUACasValue> slots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SlotKey, Integer> intSlots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ArrayKey, ConcurrentArray> arrays = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> stringsByCode = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> stringsByValue = new ConcurrentHashMap<>();
    private final AtomicInteger nextStringCode = new AtomicInteger(1);
    private final AtomicInteger nextFsId = new AtomicInteger(1);

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        return readSlot(fsRef, featureName.hashCode(), featureName);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        SlotKey key = new SlotKey(fsRef, featureCode);
        Integer intValue = intSlots.get(key);
        if (intValue != null) {
            return Optional.of(DUACasValue.ofInt(intValue));
        }
        return Optional.ofNullable(slots.get(key));
    }

    @Override
    public void writeSlot(int fsRef, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName.hashCode(), featureName, value);
    }

    @Override
    public void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        SlotKey key = new SlotKey(fsRef, featureCode);
        if (value.value() == null) {
            intSlots.remove(key);
            slots.remove(key);
        } else if (value.kind() == DUACasValueKind.INTEGER) {
            slots.remove(key);
            intSlots.put(key, value.intValue());
        } else {
            intSlots.remove(key);
            slots.put(key, value);
        }
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        DUACasValue value = slots.get(new SlotKey(fsRef, featureCode));
        if (value != null) {
            return value.intValue();
        }
        return intSlots.getOrDefault(new SlotKey(fsRef, featureCode), defaultValue);
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        SlotKey key = new SlotKey(fsRef, featureCode);
        slots.remove(key);
        intSlots.put(key, value);
    }

    @Override
    public void initializeArray(DUACasArrayKind kind, int fsRef, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative");
        }
        arrays.compute(new ArrayKey(kind, fsRef), (ignored, existing) -> {
            if (existing == null) {
                return new ConcurrentArray(length);
            }
            existing.ensureSize(length);
            return existing;
        });
    }

    @Override
    public int arraySize(DUACasArrayKind kind, int fsRef) {
        ConcurrentArray array = arrays.get(new ArrayKey(kind, fsRef));
        return array == null ? 0 : array.size();
    }

    @Override
    public Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        ConcurrentArray array = arrays.get(new ArrayKey(kind, fsRef));
        if (array == null) {
            return Optional.empty();
        }
        array.checkIndex(index);
        return Optional.ofNullable(array.values.get(index));
    }

    @Override
    public void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value) {
        arrays.computeIfAbsent(new ArrayKey(kind, fsRef), ignored -> new ConcurrentArray(index + 1))
                .write(index, value);
    }

    @Override
    public String stringForCode(int code) {
        return stringsByCode.get(code);
    }

    @Override
    public int codeForString(String value) {
        if (value == null) {
            return 0;
        }
        return stringsByValue.computeIfAbsent(value, v -> {
            int code = nextStringCode.getAndIncrement();
            stringsByCode.put(code, v);
            return code;
        });
    }

    @Override
    public int allocateFsId(int typeCode, int viewId) {
        return nextFsId.getAndIncrement();
    }

    private record SlotKey(int fsRef, int featureCode) {
    }

    private record ArrayKey(DUACasArrayKind kind, int fsRef) {
        private ArrayKey {
            Objects.requireNonNull(kind, "kind");
        }
    }

    private static final class ConcurrentArray {
        private final AtomicInteger size;
        private final ConcurrentHashMap<Integer, DUACasValue> values = new ConcurrentHashMap<>();

        private ConcurrentArray(int size) {
            this.size = new AtomicInteger(size);
        }

        private int size() {
            return size.get();
        }

        private void ensureSize(int requiredSize) {
            size.accumulateAndGet(requiredSize, Math::max);
        }

        private void write(int index, DUACasValue value) {
            ensureSize(index + 1);
            if (value.value() == null) {
                values.remove(index);
            } else {
                values.put(index, value);
            }
        }

        private void checkIndex(int index) {
            int currentSize = size.get();
            if (index < 0 || index >= currentSize) {
                throw new ArrayIndexOutOfBoundsException("index " + index + " outside array size " + currentSize);
            }
        }
    }
}

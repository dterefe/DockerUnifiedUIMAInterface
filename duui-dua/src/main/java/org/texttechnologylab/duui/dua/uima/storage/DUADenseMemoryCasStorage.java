package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

public final class DUADenseMemoryCasStorage implements DUAFastCasStorage {
    private final ConcurrentHashMap<SlotKey, DUACasValue> slots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> featureSlots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, FsIntSlots> intSlotsByFs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ArrayKey, ConcurrentArray> arrays = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> stringsByCode = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> stringsByValue = new ConcurrentHashMap<>();
    private final AtomicInteger nextFeatureSlot = new AtomicInteger();
    private final AtomicInteger nextStringCode = new AtomicInteger(1);
    private final AtomicInteger nextFsId = new AtomicInteger(1);

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        return readSlot(fsRef, featureName.hashCode(), featureName);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        FsIntSlots fsSlots = intSlotsByFs.get(fsRef);
        Integer featureSlot = featureSlots.get(featureCode);
        if (fsSlots != null && featureSlot != null && fsSlots.has(featureSlot)) {
            return Optional.of(DUACasValue.ofInt(fsSlots.read(featureSlot, 0)));
        }
        return Optional.ofNullable(slots.get(new SlotKey(fsRef, featureCode)));
    }

    @Override
    public void writeSlot(int fsRef, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName.hashCode(), featureName, value);
    }

    @Override
    public void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        SlotKey genericKey = new SlotKey(fsRef, featureCode);
        if (value.value() == null) {
            clearIntSlot(fsRef, featureCode);
            slots.remove(genericKey);
        } else if (value.kind() == DUACasValueKind.INTEGER) {
            slots.remove(genericKey);
            writeIntSlot(fsRef, featureCode, featureName, value.intValue());
        } else {
            clearIntSlot(fsRef, featureCode);
            slots.put(genericKey, value);
        }
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        Integer featureSlot = featureSlots.get(featureCode);
        FsIntSlots fsSlots = intSlotsByFs.get(fsRef);
        if (featureSlot != null && fsSlots != null) {
            return fsSlots.read(featureSlot, defaultValue);
        }
        DUACasValue value = slots.get(new SlotKey(fsRef, featureCode));
        return value == null ? defaultValue : value.intValue();
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        int featureSlot = featureSlot(featureCode);
        intSlotsByFs.computeIfAbsent(fsRef, ignored -> new FsIntSlots()).write(featureSlot, value);
        slots.remove(new SlotKey(fsRef, featureCode));
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

    public void evictFs(int fsRef) {
        intSlotsByFs.remove(fsRef);
        slots.keySet().removeIf(key -> key.fsRef() == fsRef);
        arrays.keySet().removeIf(key -> key.fsRef() == fsRef);
    }

    private int featureSlot(int featureCode) {
        return featureSlots.computeIfAbsent(featureCode, ignored -> nextFeatureSlot.getAndIncrement());
    }

    private void clearIntSlot(int fsRef, int featureCode) {
        Integer featureSlot = featureSlots.get(featureCode);
        FsIntSlots fsSlots = intSlotsByFs.get(fsRef);
        if (featureSlot != null && fsSlots != null) {
            fsSlots.clear(featureSlot);
        }
    }

    private record SlotKey(int fsRef, int featureCode) {
    }

    private record ArrayKey(DUACasArrayKind kind, int fsRef) {
        private ArrayKey {
            Objects.requireNonNull(kind, "kind");
        }
    }

    private static final class FsIntSlots {
        private volatile AtomicIntegerArray values = new AtomicIntegerArray(4);
        private volatile AtomicLongArray presentWords = new AtomicLongArray(1);

        private int read(int featureSlot, int defaultValue) {
            AtomicIntegerArray currentValues = values;
            AtomicLongArray currentPresent = presentWords;
            int wordIndex = featureSlot >>> 6;
            if (featureSlot >= currentValues.length() || wordIndex >= currentPresent.length()) {
                return defaultValue;
            }
            long mask = 1L << (featureSlot & 63);
            if ((currentPresent.get(wordIndex) & mask) == 0) {
                return defaultValue;
            }
            return currentValues.get(featureSlot);
        }

        private boolean has(int featureSlot) {
            AtomicLongArray currentPresent = presentWords;
            int wordIndex = featureSlot >>> 6;
            if (wordIndex >= currentPresent.length()) {
                return false;
            }
            long mask = 1L << (featureSlot & 63);
            return (currentPresent.get(wordIndex) & mask) != 0;
        }

        private void write(int featureSlot, int value) {
            ensureCapacity(featureSlot);
            values.set(featureSlot, value);
            int wordIndex = featureSlot >>> 6;
            long mask = 1L << (featureSlot & 63);
            presentWords.getAndUpdate(wordIndex, current -> current | mask);
        }

        private void clear(int featureSlot) {
            AtomicLongArray currentPresent = presentWords;
            int wordIndex = featureSlot >>> 6;
            if (wordIndex >= currentPresent.length()) {
                return;
            }
            long mask = ~(1L << (featureSlot & 63));
            currentPresent.getAndUpdate(wordIndex, current -> current & mask);
        }

        private synchronized void ensureCapacity(int featureSlot) {
            if (featureSlot < values.length()) {
                return;
            }
            int newLength = values.length();
            while (featureSlot >= newLength) {
                newLength *= 2;
            }
            AtomicIntegerArray oldValues = values;
            AtomicIntegerArray newValues = new AtomicIntegerArray(newLength);
            for (int i = 0; i < oldValues.length(); i++) {
                newValues.set(i, oldValues.get(i));
            }
            AtomicLongArray oldPresent = presentWords;
            AtomicLongArray newPresent = new AtomicLongArray((newLength + 63) >>> 6);
            for (int i = 0; i < oldPresent.length(); i++) {
                newPresent.set(i, oldPresent.get(i));
            }
            values = newValues;
            presentWords = newPresent;
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

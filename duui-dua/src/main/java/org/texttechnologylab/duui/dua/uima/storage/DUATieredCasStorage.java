package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class DUATieredCasStorage implements DUAFastCasStorage {
    private static final Mutation POISON = new Mutation(-1, () -> {
    });

    private final DUACasStorage durable;
    private final DUAFastCasStorage durableFast;
    private final DUADenseMemoryCasStorage hot = new DUADenseMemoryCasStorage();
    private final DUATieredWritePolicy writePolicy;
    private final int maxResidentFs;
    private final LinkedHashMap<Integer, Boolean> residentFs = new LinkedHashMap<>(16, 0.75f, true);
    private final Set<SlotKey> hotSlots = ConcurrentHashMap.newKeySet();
    private final Set<Long> hotIntSlots = ConcurrentHashMap.newKeySet();
    private final Set<ArrayKey> hotArraySizes = ConcurrentHashMap.newKeySet();
    private final Set<ArrayValueKey> hotArrayValues = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Long, PendingIntSlot> pendingIntSlots = new ConcurrentHashMap<>();
    private final Set<Integer> dirtyIntFs = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Integer, AtomicInteger> pendingByFs = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<Mutation> mutations;
    private final AtomicInteger pendingMutations = new AtomicInteger();
    private final AtomicReference<RuntimeException> writerFailure = new AtomicReference<>();
    private final Object pendingMonitor = new Object();
    private final Thread writer;
    private volatile boolean closed;

    public DUATieredCasStorage(DUACasStorage durable, int maxResidentFs, DUATieredWritePolicy writePolicy) {
        if (maxResidentFs < 1) {
            throw new IllegalArgumentException("maxResidentFs must be positive");
        }
        this.durable = Objects.requireNonNull(durable, "durable");
        this.durableFast = durable instanceof DUAFastCasStorage fast ? fast : null;
        this.maxResidentFs = maxResidentFs;
        this.writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
        if (writePolicy == DUATieredWritePolicy.WRITE_BACK) {
            this.mutations = new LinkedBlockingQueue<>();
            this.writer = Thread.ofVirtual().name("dua-tiered-writer").start(this::drainMutations);
        } else {
            this.mutations = null;
            this.writer = null;
        }
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        return readSlot(fsRef, featureName.hashCode(), featureName);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        checkUsable();
        SlotKey key = new SlotKey(fsRef, featureCode);
        if (hotSlots.contains(key) || hotIntSlots.contains(slotKey(fsRef, featureCode))) {
            return hot.readSlot(fsRef, featureCode, featureName);
        }
        Optional<DUACasValue> value = durable.readSlot(fsRef, featureCode, featureName);
        value.ifPresent(v -> {
            hot.writeSlot(fsRef, featureCode, featureName, v);
            hotSlots.add(key);
            touch(fsRef);
        });
        return value;
    }

    @Override
    public void writeSlot(int fsRef, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName.hashCode(), featureName, value);
    }

    @Override
    public void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        checkUsable();
        DUACasValue storedValue = Objects.requireNonNull(value, "value");
        SlotKey key = new SlotKey(fsRef, featureCode);
        hot.writeSlot(fsRef, featureCode, featureName, storedValue);
        hotSlots.add(key);
        touch(fsRef);
        persist(fsRef, () -> durable.writeSlot(fsRef, featureCode, featureName, storedValue));
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        checkUsable();
        int sentinel = Integer.MIN_VALUE;
        int hotValue = hot.readIntSlotOrDefault(fsRef, featureCode, featureName, sentinel);
        if (hotValue != sentinel) {
            return hotValue;
        }
        if (hotIntSlots.contains(slotKey(fsRef, featureCode))) {
            return hotValue;
        }
        Optional<DUACasValue> value = durable.readSlot(fsRef, featureCode, featureName);
        if (value.isEmpty()) {
            return defaultValue;
        }
        int intValue = value.orElseThrow().intValue();
        hot.writeIntSlot(fsRef, featureCode, featureName, intValue);
        hotIntSlots.add(slotKey(fsRef, featureCode));
        touch(fsRef);
        return intValue;
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        checkUsable();
        hot.writeIntSlot(fsRef, featureCode, featureName, value);
        long packedKey = slotKey(fsRef, featureCode);
        hotIntSlots.add(packedKey);
        if (writePolicy == DUATieredWritePolicy.WRITE_BACK) {
            pendingIntSlots.put(packedKey, new PendingIntSlot(fsRef, featureCode, featureName, value));
            dirtyIntFs.add(fsRef);
            return;
        }
        touch(fsRef);
        persist(fsRef, () -> {
            if (durableFast == null) {
                durable.writeSlot(fsRef, featureCode, featureName, DUACasValue.ofInt(value));
            } else {
                durableFast.writeIntSlot(fsRef, featureCode, featureName, value);
            }
        });
    }

    @Override
    public void initializeArray(DUACasArrayKind kind, int fsRef, int length) {
        checkUsable();
        ArrayKey key = new ArrayKey(kind, fsRef);
        hot.initializeArray(kind, fsRef, length);
        hotArraySizes.add(key);
        touch(fsRef);
        persist(fsRef, () -> durable.initializeArray(kind, fsRef, length));
    }

    @Override
    public int arraySize(DUACasArrayKind kind, int fsRef) {
        checkUsable();
        ArrayKey key = new ArrayKey(kind, fsRef);
        if (hotArraySizes.contains(key)) {
            return hot.arraySize(kind, fsRef);
        }
        int size = durable.arraySize(kind, fsRef);
        if (size > 0) {
            hot.initializeArray(kind, fsRef, size);
            hotArraySizes.add(key);
            touch(fsRef);
        }
        return size;
    }

    @Override
    public Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        checkUsable();
        ArrayValueKey key = new ArrayValueKey(kind, fsRef, index);
        if (hotArrayValues.contains(key)) {
            return hot.readArrayValue(kind, fsRef, index);
        }
        Optional<DUACasValue> value = durable.readArrayValue(kind, fsRef, index);
        value.ifPresent(v -> {
            hot.writeArrayValue(kind, fsRef, index, v);
            hotArrayValues.add(key);
            hotArraySizes.add(new ArrayKey(kind, fsRef));
            touch(fsRef);
        });
        return value;
    }

    @Override
    public void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value) {
        checkUsable();
        DUACasValue storedValue = Objects.requireNonNull(value, "value");
        hot.writeArrayValue(kind, fsRef, index, storedValue);
        hotArrayValues.add(new ArrayValueKey(kind, fsRef, index));
        hotArraySizes.add(new ArrayKey(kind, fsRef));
        touch(fsRef);
        persist(fsRef, () -> durable.writeArrayValue(kind, fsRef, index, storedValue));
    }

    @Override
    public String stringForCode(int code) {
        checkUsable();
        return durable.stringForCode(code);
    }

    @Override
    public int codeForString(String value) {
        checkUsable();
        return durable.codeForString(value);
    }

    @Override
    public int allocateFsId(int typeCode, int viewId) {
        checkUsable();
        return durable.allocateFsId(typeCode, viewId);
    }

    @Override
    public void onFsCreated(int fsRef, int typeCode, int viewId) {
        checkUsable();
        touch(fsRef);
        persist(fsRef, () -> durable.onFsCreated(fsRef, typeCode, viewId));
    }

    @Override
    public void onFsDeleted(int fsRef) {
        checkUsable();
        evictHot(fsRef);
        persist(fsRef, () -> durable.onFsDeleted(fsRef));
    }

    public void flush() {
        checkFailure();
        if (writePolicy == DUATieredWritePolicy.WRITE_THROUGH) {
            return;
        }
        while (true) {
            synchronized (pendingMonitor) {
                while (pendingMutations.get() > 0 && writerFailure.get() == null) {
                    try {
                        pendingMonitor.wait(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DUACasStorageException("Interrupted while flushing tiered CAS storage", e);
                    }
                }
            }
            checkFailure();
            if (pendingIntSlots.isEmpty()) {
                return;
            }
            for (Map.Entry<Long, PendingIntSlot> entry : pendingIntSlots.entrySet()) {
                PendingIntSlot pending = entry.getValue();
                try {
                    if (durableFast == null) {
                        durable.writeSlot(pending.fsRef(), pending.featureCode(), pending.featureName(),
                                DUACasValue.ofInt(pending.value()));
                    } else {
                        durableFast.writeIntSlot(pending.fsRef(), pending.featureCode(), pending.featureName(),
                                pending.value());
                    }
                } finally {
                    pendingIntSlots.remove(entry.getKey(), pending);
                    rememberResident(pending.fsRef());
                    removeDirtyIntFsIfClean(pending.fsRef());
                }
            }
            synchronized (residentFs) {
                evictCleanResidents();
            }
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            flush();
            if (writePolicy == DUATieredWritePolicy.WRITE_BACK) {
                mutations.add(POISON);
                writer.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DUACasStorageException("Interrupted while closing tiered CAS storage", e);
        } finally {
            durable.close();
        }
    }

    private void persist(int fsRef, Runnable mutation) {
        if (writePolicy == DUATieredWritePolicy.WRITE_THROUGH) {
            mutation.run();
            return;
        }
        if (closed) {
            throw new DUACasStorageException("Tiered CAS storage is closed");
        }
        pendingByFs.computeIfAbsent(fsRef, ignored -> new AtomicInteger()).incrementAndGet();
        pendingMutations.incrementAndGet();
        mutations.add(new Mutation(fsRef, mutation));
    }

    private void drainMutations() {
        while (true) {
            try {
                Mutation mutation = mutations.take();
                if (mutation == POISON) {
                    return;
                }
                try {
                    mutation.apply();
                } finally {
                    AtomicInteger fsPending = pendingByFs.get(mutation.fsRef());
                    if (fsPending != null && fsPending.decrementAndGet() == 0) {
                        pendingByFs.remove(mutation.fsRef(), fsPending);
                    }
                    if (pendingMutations.decrementAndGet() == 0) {
                        synchronized (pendingMonitor) {
                            pendingMonitor.notifyAll();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                writerFailure.compareAndSet(null, e);
                synchronized (pendingMonitor) {
                    pendingMonitor.notifyAll();
                }
            }
        }
    }

    private void touch(int fsRef) {
        synchronized (residentFs) {
            residentFs.put(fsRef, Boolean.TRUE);
            evictCleanResidents();
        }
    }

    private void rememberResident(int fsRef) {
        synchronized (residentFs) {
            residentFs.put(fsRef, Boolean.TRUE);
        }
    }

    private void evictCleanResidents() {
        Iterator<Map.Entry<Integer, Boolean>> iterator = residentFs.entrySet().iterator();
        while (residentFs.size() > maxResidentFs && iterator.hasNext()) {
            int candidate = iterator.next().getKey();
            if (isDirty(candidate)) {
                continue;
            }
            iterator.remove();
            evictHot(candidate);
        }
    }

    private boolean isDirty(int fsRef) {
        AtomicInteger pending = pendingByFs.get(fsRef);
        return (pending != null && pending.get() > 0) || dirtyIntFs.contains(fsRef);
    }

    private void removeDirtyIntFsIfClean(int fsRef) {
        for (PendingIntSlot pending : pendingIntSlots.values()) {
            if (pending.fsRef() == fsRef) {
                return;
            }
        }
        dirtyIntFs.remove(fsRef);
    }

    private void evictHot(int fsRef) {
        hot.evictFs(fsRef);
        hotSlots.removeIf(key -> key.fsRef() == fsRef);
        hotIntSlots.removeIf(key -> fsRef(key) == fsRef);
        hotArraySizes.removeIf(key -> key.fsRef() == fsRef);
        hotArrayValues.removeIf(key -> key.fsRef() == fsRef);
    }

    private void checkUsable() {
        checkFailure();
        if (closed) {
            throw new DUACasStorageException("Tiered CAS storage is closed");
        }
    }

    private void checkFailure() {
        RuntimeException failure = writerFailure.get();
        if (failure != null) {
            throw new DUACasStorageException("Tiered CAS storage writer failed", failure);
        }
    }

    private record Mutation(int fsRef, Runnable action) {
        private void apply() {
            action.run();
        }
    }

    private record PendingIntSlot(int fsRef, int featureCode, String featureName, int value) {
    }

    private static long slotKey(int fsRef, int featureCode) {
        return (((long) fsRef) << 32) ^ (featureCode & 0xffff_ffffL);
    }

    private static int fsRef(long slotKey) {
        return (int) (slotKey >> 32);
    }

    private record SlotKey(int fsRef, int featureCode) {
    }

    private record ArrayKey(DUACasArrayKind kind, int fsRef) {
    }

    private record ArrayValueKey(DUACasArrayKind kind, int fsRef, int index) {
    }
}

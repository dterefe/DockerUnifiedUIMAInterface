package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Objects;

import org.apache.uima.cas.Feature;
import org.apache.uima.cas.impl.Backend;
import org.apache.uima.cas.impl.FeatureImpl;
import org.texttechnologylab.duui.dua.store.DUAAnnotationIndex;
import org.texttechnologylab.duui.dua.store.DUAStoreBundle;
import org.texttechnologylab.duui.dua.store.DUATextQueryStore;
import org.texttechnologylab.duui.dua.store.DUATypesystemIndex;
import org.texttechnologylab.duui.dua.store.DUAValueQueryStore;

public final class DUAStorageBackend implements Backend {
    private final DUACasStorage storage;
    private final DUAFastCasStorage fastStorage;
    private final DUAStoreBundle stores;
    private final SlotBackend slots = new Slots();
    private final ArrayBackend arrays = new Arrays();
    private final StringBackend strings = new Strings();
    private final LifecycleBackend lifecycle = new Lifecycle();
    private final CollectionBackend collections = new Collections();

    public DUAStorageBackend(DUACasStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.fastStorage = storage instanceof DUAFastCasStorage fast ? fast : null;
        this.stores = null;
    }

    public DUAStorageBackend(DUACasStorage storage, DUAStoreBundle stores) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.fastStorage = storage instanceof DUAFastCasStorage fast ? fast : null;
        this.stores = Objects.requireNonNull(stores, "stores");
    }

    public DUACasStorage storage() {
        return storage;
    }

    public DUAStoreBundle stores() {
        return stores;
    }

    // ── Store accessors ─────────────────────────────────────────────────────

    public DUAAnnotationIndex annotationIndex() {
        if (stores == null) {
            throw new IllegalStateException("No DUAStoreBundle available");
        }
        return stores.annotationIndex();
    }

    public DUATypesystemIndex typesystemIndex() {
        if (stores == null) {
            throw new IllegalStateException("No DUAStoreBundle available");
        }
        return stores.typesystemIndex();
    }

    public DUAValueQueryStore values() {
        if (stores == null) {
            throw new IllegalStateException("No DUAStoreBundle available");
        }
        return stores.values();
    }

    public DUATextQueryStore texts() {
        if (stores == null) {
            throw new IllegalStateException("No DUAStoreBundle available");
        }
        return stores.texts();
    }

    // ── Backend interface sub-backend accessors ─────────────────────────────

    @Override
    public SlotBackend slots() {
        return slots;
    }

    @Override
    public ArrayBackend arrays() {
        return arrays;
    }

    @Override
    public CollectionBackend collections() {
        return collections;
    }

    @Override
    public StringBackend strings() {
        return strings;
    }

    @Override
    public LifecycleBackend lifecycle() {
        return lifecycle;
    }

    private static int featureCode(Feature feature) {
        return feature instanceof FeatureImpl featureImpl ? featureImpl.getCode() : feature.getName().hashCode();
    }

    private final class Slots implements SlotBackend {
        @Override public boolean getBooleanValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::booleanValue).orElse(false); }
        @Override public void setBooleanValue(int fsRef, Feature feature, boolean value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public byte getByteValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::byteValue).orElse((byte) 0); }
        @Override public void setByteValue(int fsRef, Feature feature, byte value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public short getShortValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::shortValue).orElse((short) 0); }
        @Override public void setShortValue(int fsRef, Feature feature, short value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public int getIntValue(int fsRef, Feature feature) {
            int featureCode = featureCode(feature);
            return fastStorage == null
                    ? storage.readSlot(fsRef, featureCode, feature.getName()).map(DUACasValue::intValue).orElse(0)
                    : fastStorage.readIntSlotOrDefault(fsRef, featureCode, feature.getName(), 0);
        }
        @Override public void setIntValue(int fsRef, Feature feature, int value) {
            int featureCode = featureCode(feature);
            if (fastStorage == null) {
                storage.writeSlot(fsRef, featureCode, feature.getName(), DUACasValue.ofInt(value));
            } else {
                fastStorage.writeIntSlot(fsRef, featureCode, feature.getName(), value);
            }
        }
        @Override public long getLongValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::longValue).orElse(0L); }
        @Override public void setLongValue(int fsRef, Feature feature, long value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.ofLong(value)); }
        @Override public float getFloatValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::floatValue).orElse(0.0f); }
        @Override public void setFloatValue(int fsRef, Feature feature, float value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public double getDoubleValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::doubleValue).orElse(0.0d); }
        @Override public void setDoubleValue(int fsRef, Feature feature, double value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public String getStringValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::stringValue).orElse(null); }
        @Override public void setStringValue(int fsRef, Feature feature, String value) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.of(value)); }
        @Override public int getRefValue(int fsRef, Feature feature) { return storage.readSlot(fsRef, featureCode(feature), feature.getName()).map(DUACasValue::intValue).orElse(0); }
        @Override public void setRefValue(int fsRef, Feature feature, int targetFsRef) { storage.writeSlot(fsRef, featureCode(feature), feature.getName(), DUACasValue.ref(targetFsRef)); }
    }

    private final class Arrays implements ArrayBackend {
        @Override public void initializeFsArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.FS, fsRef, length); }
        @Override public int sizeFsArray(int fsRef) { return storage.arraySize(DUACasArrayKind.FS, fsRef); }
        @Override public int getFsArrayRefValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.FS, fsRef, index).map(DUACasValue::intValue).orElse(0); }
        @Override public void setFsArrayRefValue(int fsRef, int index, int targetFsRef) { storage.writeArrayValue(DUACasArrayKind.FS, fsRef, index, DUACasValue.ref(targetFsRef)); }
        @Override public void copyFromFsArray(int fsRef, int destPos, int[] srcFsRefs, int srcPos, int length) { for (int i = 0; i < length; i++) setFsArrayRefValue(fsRef, destPos + i, srcFsRefs[srcPos + i]); }
        @Override public void copyToFsArray(int fsRef, int srcPos, int[] destFsRefs, int destPos, int length) { for (int i = 0; i < length; i++) destFsRefs[destPos + i] = getFsArrayRefValue(fsRef, srcPos + i); }
        @Override public void initializeIntegerArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.INTEGER, fsRef, length); }
        @Override public int sizeIntegerArray(int fsRef) { return storage.arraySize(DUACasArrayKind.INTEGER, fsRef); }
        @Override public int getIntegerArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.INTEGER, fsRef, index).map(DUACasValue::intValue).orElse(0); }
        @Override public void setIntegerArrayValue(int fsRef, int index, int value) { storage.writeArrayValue(DUACasArrayKind.INTEGER, fsRef, index, DUACasValue.ofInt(value)); }
        @Override public void copyFromIntegerArray(int fsRef, int destPos, int[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setIntegerArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToIntegerArray(int fsRef, int srcPos, int[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getIntegerArrayValue(fsRef, srcPos + i); }
        @Override public void initializeFloatArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.FLOAT, fsRef, length); }
        @Override public int sizeFloatArray(int fsRef) { return storage.arraySize(DUACasArrayKind.FLOAT, fsRef); }
        @Override public float getFloatArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.FLOAT, fsRef, index).map(DUACasValue::floatValue).orElse(0.0f); }
        @Override public void setFloatArrayValue(int fsRef, int index, float value) { storage.writeArrayValue(DUACasArrayKind.FLOAT, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromFloatArray(int fsRef, int destPos, float[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setFloatArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToFloatArray(int fsRef, int srcPos, float[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getFloatArrayValue(fsRef, srcPos + i); }
        @Override public void initializeStringArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.STRING, fsRef, length); }
        @Override public int sizeStringArray(int fsRef) { return storage.arraySize(DUACasArrayKind.STRING, fsRef); }
        @Override public String getStringArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.STRING, fsRef, index).map(DUACasValue::stringValue).orElse(null); }
        @Override public void setStringArrayValue(int fsRef, int index, String value) { storage.writeArrayValue(DUACasArrayKind.STRING, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromStringArray(int fsRef, int destPos, String[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setStringArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToStringArray(int fsRef, int srcPos, String[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getStringArrayValue(fsRef, srcPos + i); }
        @Override public void initializeBooleanArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.BOOLEAN, fsRef, length); }
        @Override public int sizeBooleanArray(int fsRef) { return storage.arraySize(DUACasArrayKind.BOOLEAN, fsRef); }
        @Override public boolean getBooleanArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.BOOLEAN, fsRef, index).map(DUACasValue::booleanValue).orElse(false); }
        @Override public void setBooleanArrayValue(int fsRef, int index, boolean value) { storage.writeArrayValue(DUACasArrayKind.BOOLEAN, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromBooleanArray(int fsRef, int destPos, boolean[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setBooleanArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToBooleanArray(int fsRef, int srcPos, boolean[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getBooleanArrayValue(fsRef, srcPos + i); }
        @Override public void initializeByteArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.BYTE, fsRef, length); }
        @Override public int sizeByteArray(int fsRef) { return storage.arraySize(DUACasArrayKind.BYTE, fsRef); }
        @Override public byte getByteArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.BYTE, fsRef, index).map(DUACasValue::byteValue).orElse((byte) 0); }
        @Override public void setByteArrayValue(int fsRef, int index, byte value) { storage.writeArrayValue(DUACasArrayKind.BYTE, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromByteArray(int fsRef, int destPos, byte[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setByteArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToByteArray(int fsRef, int srcPos, byte[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getByteArrayValue(fsRef, srcPos + i); }
        @Override public void initializeShortArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.SHORT, fsRef, length); }
        @Override public int sizeShortArray(int fsRef) { return storage.arraySize(DUACasArrayKind.SHORT, fsRef); }
        @Override public short getShortArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.SHORT, fsRef, index).map(DUACasValue::shortValue).orElse((short) 0); }
        @Override public void setShortArrayValue(int fsRef, int index, short value) { storage.writeArrayValue(DUACasArrayKind.SHORT, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromShortArray(int fsRef, int destPos, short[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setShortArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToShortArray(int fsRef, int srcPos, short[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getShortArrayValue(fsRef, srcPos + i); }
        @Override public void initializeLongArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.LONG, fsRef, length); }
        @Override public int sizeLongArray(int fsRef) { return storage.arraySize(DUACasArrayKind.LONG, fsRef); }
        @Override public long getLongArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.LONG, fsRef, index).map(DUACasValue::longValue).orElse(0L); }
        @Override public void setLongArrayValue(int fsRef, int index, long value) { storage.writeArrayValue(DUACasArrayKind.LONG, fsRef, index, DUACasValue.ofLong(value)); }
        @Override public void copyFromLongArray(int fsRef, int destPos, long[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setLongArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToLongArray(int fsRef, int srcPos, long[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getLongArrayValue(fsRef, srcPos + i); }
        @Override public void initializeDoubleArray(int fsRef, int length) { storage.initializeArray(DUACasArrayKind.DOUBLE, fsRef, length); }
        @Override public int sizeDoubleArray(int fsRef) { return storage.arraySize(DUACasArrayKind.DOUBLE, fsRef); }
        @Override public double getDoubleArrayValue(int fsRef, int index) { return storage.readArrayValue(DUACasArrayKind.DOUBLE, fsRef, index).map(DUACasValue::doubleValue).orElse(0.0d); }
        @Override public void setDoubleArrayValue(int fsRef, int index, double value) { storage.writeArrayValue(DUACasArrayKind.DOUBLE, fsRef, index, DUACasValue.of(value)); }
        @Override public void copyFromDoubleArray(int fsRef, int destPos, double[] src, int srcPos, int length) { for (int i = 0; i < length; i++) setDoubleArrayValue(fsRef, destPos + i, src[srcPos + i]); }
        @Override public void copyToDoubleArray(int fsRef, int srcPos, double[] dest, int destPos, int length) { for (int i = 0; i < length; i++) dest[destPos + i] = getDoubleArrayValue(fsRef, srcPos + i); }
    }

    private final class Strings implements StringBackend {
        @Override public String getStringForCode(int code) { return storage.stringForCode(code); }
        @Override public int getCodeForString(String value) { return storage.codeForString(value); }
    }

    private final class Lifecycle implements LifecycleBackend {
        @Override public int allocateFsId(int typeCode, int viewId) { return storage.allocateFsId(typeCode, viewId); }
        @Override public void onFsCreated(int fsRef, int typeCode, int viewId) { storage.onFsCreated(fsRef, typeCode, viewId); }
        @Override public void onFsDeleted(int fsRef) { storage.onFsDeleted(fsRef); }
    }

    private static final class Collections implements CollectionBackend {
        @Override public void initFsArrayListFromCasData(int fsRef) {}
        @Override public void saveFsArrayListToCasData(int fsRef) {}
        @Override public void initFsHashSetFromCasData(int fsRef) {}
        @Override public void saveFsHashSetToCasData(int fsRef) {}
        @Override public void initInt2FsFromCasData(int fsRef) {}
        @Override public void saveInt2FsToCasData(int fsRef) {}
        @Override public void initIntegerArrayListFromCasData(int fsRef) {}
        @Override public void saveIntegerArrayListToCasData(int fsRef) {}
    }
}

package org.texttechnologylab.duui.dua.uima.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUAOrderedKvCasStorage implements DUAFastCasStorage {
    private static final byte P_LIFECYCLE = 0x01;
    private static final byte P_BOOL_SLOT = 0x02;
    private static final byte P_I64_SLOT = 0x03;
    private static final byte P_F32_SLOT = 0x04;
    private static final byte P_F64_SLOT = 0x05;
    private static final byte P_STRING_SLOT = 0x06;
    private static final byte P_REF_SLOT = 0x07;
    private static final byte P_ARRAY_META = 0x08;
    private static final byte P_BOOL_ARRAY = 0x09;
    private static final byte P_I64_ARRAY = 0x0A;
    private static final byte P_F32_ARRAY = 0x0B;
    private static final byte P_F64_ARRAY = 0x0C;
    private static final byte P_STRING_ARRAY = 0x0D;
    private static final byte P_REF_ARRAY = 0x0E;
    private static final byte P_STRING_CODE = 0x0F;
    private static final byte P_STRING_VALUE = 0x10;
    private static final byte P_COUNTER = 0x11;

    private static final String WAL_FILE = "cas-kv-v1.wal";
    private static final String NEXT_FS_ID = "next_fs_id";
    private static final String NEXT_STRING_CODE = "next_string_code";

    private final ConcurrentSkipListMap<Key, byte[]> records = new ConcurrentSkipListMap<>();
    private final Path walPath;
    private final DataOutputStream wal;
    private final AtomicBoolean closed = new AtomicBoolean();

    public DUAOrderedKvCasStorage(Path directory) {
        try {
            Files.createDirectories(directory);
            this.walPath = directory.resolve(WAL_FILE);
            loadWal();
            this.wal = new DataOutputStream(Files.newOutputStream(walPath,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE));
            initializeCounter(NEXT_FS_ID, 1);
            initializeCounter(NEXT_STRING_CODE, 1);
        } catch (IOException e) {
            throw new DUACasStorageException("Could not open ordered KV CAS storage", e);
        }
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, String featureName) {
        return readSlot(fsRef, featureName.hashCode(), featureName);
    }

    @Override
    public Optional<DUACasValue> readSlot(int fsRef, int featureCode, String featureName) {
        long featureKey = featureKey(featureCode, featureName);
        byte[] value = get(key(P_BOOL_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(DUACasValue.of(value[0] != 0));
        }
        value = get(key(P_I64_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(decodeI64(value));
        }
        value = get(key(P_F32_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(DUACasValue.of(Float.intBitsToFloat(ByteBuffer.wrap(value).getInt())));
        }
        value = get(key(P_F64_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(DUACasValue.of(Double.longBitsToDouble(ByteBuffer.wrap(value).getLong())));
        }
        value = get(key(P_STRING_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(DUACasValue.of(stringForCode((int) readU64(value))));
        }
        value = get(key(P_REF_SLOT, u64(fsRef), u64(featureKey)));
        if (value != null) {
            return Optional.of(DUACasValue.ref((int) readU64(value)));
        }
        return Optional.empty();
    }

    @Override
    public void writeSlot(int fsRef, String featureName, DUACasValue value) {
        writeSlot(fsRef, featureName.hashCode(), featureName, value);
    }

    @Override
    public synchronized void writeSlot(int fsRef, int featureCode, String featureName, DUACasValue value) {
        long featureKey = featureKey(featureCode, featureName);
        deleteSlot(fsRef, featureKey);
        if (value.value() == null) {
            return;
        }
        switch (value.kind()) {
            case BOOLEAN -> put(key(P_BOOL_SLOT, u64(fsRef), u64(featureKey)), new byte[] {(byte) (value.booleanValue() ? 1 : 0)});
            case BYTE, SHORT, INTEGER, LONG -> put(key(P_I64_SLOT, u64(fsRef), u64(featureKey)),
                    join(new byte[] {kindCode(value.kind())}, i64(value.longValue())));
            case FLOAT -> put(key(P_F32_SLOT, u64(fsRef), u64(featureKey)),
                    i32(Float.floatToRawIntBits(value.floatValue())));
            case DOUBLE -> put(key(P_F64_SLOT, u64(fsRef), u64(featureKey)),
                    i64(Double.doubleToRawLongBits(value.doubleValue())));
            case STRING -> put(key(P_STRING_SLOT, u64(fsRef), u64(featureKey)),
                    u64(codeForString(value.stringValue())));
            case REF -> put(key(P_REF_SLOT, u64(fsRef), u64(featureKey)), u64(value.intValue()));
        }
    }

    @Override
    public int readIntSlotOrDefault(int fsRef, int featureCode, String featureName, int defaultValue) {
        byte[] value = get(key(P_I64_SLOT, u64(fsRef), u64(featureKey(featureCode, featureName))));
        return value == null ? defaultValue : decodeI64(value).intValue();
    }

    @Override
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        writeSlot(fsRef, featureCode, featureName, DUACasValue.ofInt(value));
    }

    @Override
    public synchronized void initializeArray(DUACasArrayKind kind, int fsRef, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative");
        }
        int current = arraySize(kind, fsRef);
        put(key(P_ARRAY_META, new byte[] {arrayKindCode(kind)}, u64(fsRef)), u32(Math.max(current, length)));
    }

    @Override
    public int arraySize(DUACasArrayKind kind, int fsRef) {
        byte[] value = get(key(P_ARRAY_META, new byte[] {arrayKindCode(kind)}, u64(fsRef)));
        return value == null ? 0 : ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    @Override
    public Optional<DUACasValue> readArrayValue(DUACasArrayKind kind, int fsRef, int index) {
        checkArrayIndex(kind, fsRef, index);
        byte prefix = arrayPrefix(kind);
        byte[] value = get(key(prefix, new byte[] {arrayKindCode(kind)}, u64(fsRef), u32(index)));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(decodeArrayValue(kind, value));
    }

    @Override
    public synchronized void writeArrayValue(DUACasArrayKind kind, int fsRef, int index, DUACasValue value) {
        if (index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        initializeArray(kind, fsRef, index + 1);
        byte[] key = key(arrayPrefix(kind), new byte[] {arrayKindCode(kind)}, u64(fsRef), u32(index));
        if (value.value() == null) {
            delete(key);
        } else {
            put(key, encodeArrayValue(kind, value));
        }
    }

    @Override
    public String stringForCode(int code) {
        byte[] value = get(key(P_STRING_CODE, u64(code)));
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public synchronized int codeForString(String value) {
        if (value == null) {
            return 0;
        }
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        byte[] valueKey = key(P_STRING_VALUE, u64(hash64(utf8)), utf8);
        byte[] existing = get(valueKey);
        if (existing != null) {
            return (int) readU64(existing);
        }
        int code = nextCounter(NEXT_STRING_CODE);
        put(valueKey, u64(code));
        put(key(P_STRING_CODE, u64(code)), utf8);
        return code;
    }

    @Override
    public synchronized int allocateFsId(int typeCode, int viewId) {
        return nextCounter(NEXT_FS_ID);
    }

    @Override
    public void onFsCreated(int fsRef, int typeCode, int viewId) {
        put(key(P_LIFECYCLE, u64(fsRef)), join(i32(typeCode), i32(viewId), new byte[] {0}));
    }

    @Override
    public void onFsDeleted(int fsRef) {
        byte[] existing = get(key(P_LIFECYCLE, u64(fsRef)));
        if (existing == null || existing.length < 9) {
            put(key(P_LIFECYCLE, u64(fsRef)), join(i32(0), i32(0), new byte[] {1}));
            return;
        }
        byte[] deleted = Arrays.copyOf(existing, existing.length);
        deleted[8] = (byte) (deleted[8] | 1);
        put(key(P_LIFECYCLE, u64(fsRef)), deleted);
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                wal.flush();
                wal.close();
            } catch (IOException e) {
                throw new DUACasStorageException("Could not close ordered KV CAS storage", e);
            }
        }
    }

    private void loadWal() throws IOException {
        if (!Files.exists(walPath)) {
            return;
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(walPath))) {
            while (true) {
                int keyLength = input.readInt();
                int valueLength = input.readInt();
                byte[] key = input.readNBytes(keyLength);
                if (key.length != keyLength) {
                    throw new EOFException();
                }
                if (valueLength < 0) {
                    records.remove(new Key(key));
                } else {
                    byte[] value = input.readNBytes(valueLength);
                    if (value.length != valueLength) {
                        throw new EOFException();
                    }
                    records.put(new Key(key), value);
                }
            }
        } catch (EOFException ignored) {
            // Truncated tail is ignored for this v1 local WAL; future segments carry CRCs.
        }
    }

    private void initializeCounter(String name, long initialValue) {
        byte[] counterKey = key(P_COUNTER, name.getBytes(StandardCharsets.UTF_8));
        if (get(counterKey) == null) {
            put(counterKey, u64(initialValue));
        }
    }

    private int nextCounter(String name) {
        byte[] counterKey = key(P_COUNTER, name.getBytes(StandardCharsets.UTF_8));
        long next = Optional.ofNullable(get(counterKey)).map(DUAOrderedKvCasStorage::readU64).orElse(1L);
        put(counterKey, u64(next + 1));
        return Math.toIntExact(next);
    }

    private byte[] get(byte[] key) {
        return records.get(new Key(key));
    }

    private synchronized void put(byte[] key, byte[] value) {
        ensureOpen();
        byte[] stored = Arrays.copyOf(value, value.length);
        records.put(new Key(key), stored);
        append(key, stored);
    }

    private synchronized void delete(byte[] key) {
        ensureOpen();
        records.remove(new Key(key));
        append(key, null);
    }

    private void append(byte[] key, byte[] value) {
        try {
            wal.writeInt(key.length);
            wal.writeInt(value == null ? -1 : value.length);
            wal.write(key);
            if (value != null) {
                wal.write(value);
            }
            wal.flush();
        } catch (IOException e) {
            throw new DUACasStorageException("Could not append ordered KV WAL record", e);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DUACasStorageException("Ordered KV CAS storage is closed");
        }
    }

    private void deleteSlot(int fsRef, long featureKey) {
        delete(key(P_BOOL_SLOT, u64(fsRef), u64(featureKey)));
        delete(key(P_I64_SLOT, u64(fsRef), u64(featureKey)));
        delete(key(P_F32_SLOT, u64(fsRef), u64(featureKey)));
        delete(key(P_F64_SLOT, u64(fsRef), u64(featureKey)));
        delete(key(P_STRING_SLOT, u64(fsRef), u64(featureKey)));
        delete(key(P_REF_SLOT, u64(fsRef), u64(featureKey)));
    }

    private static DUACasValue decodeI64(byte[] value) {
        int kind = value[0] & 0xFF;
        long number = ByteBuffer.wrap(value, 1, 8).order(ByteOrder.BIG_ENDIAN).getLong();
        return switch (kind) {
            case 2 -> DUACasValue.of((byte) number);
            case 3 -> DUACasValue.of((short) number);
            case 4 -> DUACasValue.ofInt((int) number);
            case 5 -> DUACasValue.ofLong(number);
            default -> DUACasValue.ofLong(number);
        };
    }

    private DUACasValue decodeArrayValue(DUACasArrayKind kind, byte[] value) {
        return switch (kind) {
            case BOOLEAN -> DUACasValue.of(value[0] != 0);
            case BYTE -> DUACasValue.of((byte) ByteBuffer.wrap(value).getLong());
            case SHORT -> DUACasValue.of((short) ByteBuffer.wrap(value).getLong());
            case INTEGER -> DUACasValue.ofInt((int) ByteBuffer.wrap(value).getLong());
            case LONG -> DUACasValue.ofLong(ByteBuffer.wrap(value).getLong());
            case FLOAT -> DUACasValue.of(Float.intBitsToFloat(ByteBuffer.wrap(value).getInt()));
            case DOUBLE -> DUACasValue.of(Double.longBitsToDouble(ByteBuffer.wrap(value).getLong()));
            case STRING -> DUACasValue.of(stringForCode((int) readU64(value)));
            case FS -> DUACasValue.ref((int) readU64(value));
        };
    }

    private byte[] encodeArrayValue(DUACasArrayKind kind, DUACasValue value) {
        return switch (kind) {
            case BOOLEAN -> new byte[] {(byte) (value.booleanValue() ? 1 : 0)};
            case BYTE, SHORT, INTEGER, LONG -> i64(value.longValue());
            case FLOAT -> i32(Float.floatToRawIntBits(value.floatValue()));
            case DOUBLE -> i64(Double.doubleToRawLongBits(value.doubleValue()));
            case STRING -> u64(codeForString(value.stringValue()));
            case FS -> u64(value.intValue());
        };
    }

    private void checkArrayIndex(DUACasArrayKind kind, int fsRef, int index) {
        int length = arraySize(kind, fsRef);
        if (length == 0) {
            return;
        }
        if (index < 0 || index >= length) {
            throw new ArrayIndexOutOfBoundsException("index " + index + " outside array size " + length);
        }
    }

    private static byte arrayPrefix(DUACasArrayKind kind) {
        return switch (kind) {
            case BOOLEAN -> P_BOOL_ARRAY;
            case BYTE, SHORT, INTEGER, LONG -> P_I64_ARRAY;
            case FLOAT -> P_F32_ARRAY;
            case DOUBLE -> P_F64_ARRAY;
            case STRING -> P_STRING_ARRAY;
            case FS -> P_REF_ARRAY;
        };
    }

    private static byte arrayKindCode(DUACasArrayKind kind) {
        return (byte) switch (kind) {
            case FS -> 1;
            case INTEGER -> 2;
            case FLOAT -> 3;
            case STRING -> 4;
            case BOOLEAN -> 5;
            case BYTE -> 6;
            case SHORT -> 7;
            case LONG -> 8;
            case DOUBLE -> 9;
        };
    }

    private static byte kindCode(DUACasValueKind kind) {
        return (byte) switch (kind) {
            case BYTE -> 2;
            case SHORT -> 3;
            case INTEGER -> 4;
            case LONG -> 5;
            default -> 4;
        };
    }

    private static long featureKey(int featureCode, String featureName) {
        return featureCode == 0 ? hash64(featureName.getBytes(StandardCharsets.UTF_8)) : Integer.toUnsignedLong(featureCode);
    }

    private static long hash64(byte[] bytes) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : bytes) {
            hash ^= b & 0xFFL;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static long readU64(byte[] value) {
        return ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static byte[] key(byte prefix, byte[]... parts) {
        int length = 1;
        for (byte[] part : parts) {
            length += part.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(prefix);
        for (byte[] part : parts) {
            buffer.put(part);
        }
        return buffer.array();
    }

    private static byte[] u32(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static byte[] i32(int value) {
        return u32(value);
    }

    private static byte[] u64(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    private static byte[] i64(long value) {
        return u64(value);
    }

    private static byte[] join(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (byte[] part : parts) {
            buffer.put(part);
        }
        return buffer.array();
    }

    private record Key(byte[] bytes) implements Comparable<Key> {
        private Key {
            bytes = Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public int compareTo(Key other) {
            return Arrays.compareUnsigned(bytes, other.bytes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Key other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}

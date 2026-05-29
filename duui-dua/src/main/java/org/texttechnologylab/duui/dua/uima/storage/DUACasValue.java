package org.texttechnologylab.duui.dua.uima.storage;

import java.util.Objects;

public record DUACasValue(DUACasValueKind kind, Object value) {
    public DUACasValue {
        Objects.requireNonNull(kind, "kind");
    }

    public static DUACasValue of(boolean value) {
        return new DUACasValue(DUACasValueKind.BOOLEAN, value);
    }

    public static DUACasValue of(byte value) {
        return new DUACasValue(DUACasValueKind.BYTE, value);
    }

    public static DUACasValue of(short value) {
        return new DUACasValue(DUACasValueKind.SHORT, value);
    }

    public static DUACasValue ofInt(int value) {
        return new DUACasValue(DUACasValueKind.INTEGER, value);
    }

    public static DUACasValue ofLong(long value) {
        return new DUACasValue(DUACasValueKind.LONG, value);
    }

    public static DUACasValue of(float value) {
        return new DUACasValue(DUACasValueKind.FLOAT, value);
    }

    public static DUACasValue of(double value) {
        return new DUACasValue(DUACasValueKind.DOUBLE, value);
    }

    public static DUACasValue of(String value) {
        return new DUACasValue(DUACasValueKind.STRING, value);
    }

    public static DUACasValue ref(int fsRef) {
        return new DUACasValue(DUACasValueKind.REF, fsRef);
    }

    public boolean booleanValue() {
        return value instanceof Boolean b ? b : false;
    }

    public byte byteValue() {
        return value instanceof Number n ? n.byteValue() : 0;
    }

    public short shortValue() {
        return value instanceof Number n ? n.shortValue() : 0;
    }

    public int intValue() {
        return value instanceof Number n ? n.intValue() : 0;
    }

    public long longValue() {
        return value instanceof Number n ? n.longValue() : 0L;
    }

    public float floatValue() {
        return value instanceof Number n ? n.floatValue() : 0.0f;
    }

    public double doubleValue() {
        return value instanceof Number n ? n.doubleValue() : 0.0d;
    }

    public String stringValue() {
        return value instanceof String s ? s : null;
    }
}

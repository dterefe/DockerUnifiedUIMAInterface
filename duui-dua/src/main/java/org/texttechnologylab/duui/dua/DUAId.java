package org.texttechnologylab.duui.dua;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public final class DUAId implements Comparable<DUAId> {
    private static final SecureRandom RANDOM = new SecureRandom();
    private final String value;

    private DUAId(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static DUAId create() {
        byte[] random = new byte[10];
        RANDOM.nextBytes(random);
        StringBuilder builder = new StringBuilder(32);
        builder.append(String.format(Locale.ROOT, "%012x", Instant.now().toEpochMilli()));
        for (byte b : random) {
            builder.append(String.format(Locale.ROOT, "%02x", b));
        }
        return new DUAId(builder.toString());
    }

    public static DUAId of(String value) {
        return new DUAId(value);
    }

    public String value() {
        return value;
    }

    @Override
    public int compareTo(DUAId other) {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DUAId id && value.equals(id.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}

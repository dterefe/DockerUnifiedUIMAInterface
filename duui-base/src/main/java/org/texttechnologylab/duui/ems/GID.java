package org.texttechnologylab.duui.ems;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public final class GID implements Comparable<GID> {
    private static final SecureRandom RANDOM = new SecureRandom();
    private final String value;

    private GID(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static GID create() {
        byte[] random = new byte[10];
        RANDOM.nextBytes(random);
        StringBuilder builder = new StringBuilder(32);
        builder.append(String.format(Locale.ROOT, "%012x", Instant.now().toEpochMilli()));
        for (byte b : random) {
            builder.append(String.format(Locale.ROOT, "%02x", b));
        }
        return new GID(builder.toString());
    }

    public static GID of(String value) {
        return new GID(value);
    }

    public String value() {
        return value;
    }

    @Override
    public int compareTo(GID other) {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof GID gid && value.equals(gid.value);
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

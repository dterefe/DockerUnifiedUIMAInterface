package org.texttechnologylab.duui.ems;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

public final class GID implements Comparable<GID> {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final char[] CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
    private static final Pattern ULID = Pattern.compile("[0-9A-HJKMNP-TV-Z]{26}");
    private final String value;
    private final String type;
    private final String ulid;

    private GID(String type, String ulid) {
        this.type = validateType(type);
        this.ulid = validateUlid(ulid);
        this.value = this.type + ":" + this.ulid;
    }

    private GID(String value) {
        this.value = Objects.requireNonNull(value, "value");
        int separator = value.indexOf(':');
        if (separator > 0 && separator < value.length() - 1) {
            this.type = validateType(value.substring(0, separator));
            this.ulid = validateUlid(value.substring(separator + 1));
        } else {
            throw new IllegalArgumentException("GID must consist of type and ULID");
        }
    }

    public static GID create(Object owner) {
        Objects.requireNonNull(owner, "owner");
        return owner instanceof Class<?> type ? create(type) : create(owner.getClass());
    }

    public static GID create(Class<?> type) {
        Objects.requireNonNull(type, "type");
        return create(type.getSimpleName());
    }

    public static GID create(String type) {
        return new GID(type, generateUlid());
    }

    private static String generateUlid() {
        byte[] random = new byte[10];
        RANDOM.nextBytes(random);
        byte[] bytes = new byte[16];
        long time = Instant.now().toEpochMilli();
        bytes[0] = (byte) (time >>> 40);
        bytes[1] = (byte) (time >>> 32);
        bytes[2] = (byte) (time >>> 24);
        bytes[3] = (byte) (time >>> 16);
        bytes[4] = (byte) (time >>> 8);
        bytes[5] = (byte) time;
        System.arraycopy(random, 0, bytes, 6, random.length);
        return encodeUlid(bytes);
    }

    public static GID of(String value) {
        return new GID(value);
    }

    public static GID of(String type, String ulid) {
        return new GID(type, ulid);
    }

    public String type() {
        return type;
    }

    public String ulid() {
        return ulid;
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

    private static String validateType(String type) {
        String normalized = Objects.requireNonNull(type, "type").trim();
        if (normalized.isEmpty() || normalized.indexOf(':') >= 0) {
            throw new IllegalArgumentException("GID type must be a class name");
        }
        return normalized;
    }

    private static String validateUlid(String ulid) {
        String normalized = Objects.requireNonNull(ulid, "ulid").trim().toUpperCase(Locale.ROOT);
        if (!ULID.matcher(normalized).matches()) {
            throw new IllegalArgumentException("GID ULID must be a 26 character Crockford Base32 ULID");
        }
        return normalized;
    }

    private static String encodeUlid(byte[] bytes) {
        String binary = String.format(Locale.ROOT, "%8s", Integer.toBinaryString(bytes[0] & 0xFF)).replace(' ', '0');
        StringBuilder bits = new StringBuilder(130);
        bits.append("00").append(binary);
        for (int i = 1; i < bytes.length; i++) {
            bits.append(String.format(Locale.ROOT, "%8s", Integer.toBinaryString(bytes[i] & 0xFF)).replace(' ', '0'));
        }
        StringBuilder encoded = new StringBuilder(26);
        for (int i = 0; i < 26; i++) {
            int value = Integer.parseInt(bits.substring(i * 5, i * 5 + 5), 2);
            encoded.append(CROCKFORD[value]);
        }
        return encoded.toString();
    }
}

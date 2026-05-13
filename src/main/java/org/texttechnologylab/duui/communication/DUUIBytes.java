package org.texttechnologylab.duui.communication;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class DUUIBytes {
    private DUUIBytes() {
    }

    public static byte[] readAllBytes(InputStream input) throws IOException {
        return input.readAllBytes();
    }

    public static byte[] readNBytes(InputStream input, int length) throws IOException {
        byte[] bytes = input.readNBytes(length);
        return bytes.length == length ? bytes : null;
    }

    public static int length(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    public static int unsignedByte(byte[] bytes, int index) {
        return bytes[index] & 0xFF;
    }

    public static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] toUtf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromUtf8Bytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}

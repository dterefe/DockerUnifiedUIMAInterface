package org.texttechnologylab.duui.communication;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class DUUIOutputStream extends FilterOutputStream {
    public DUUIOutputStream(OutputStream output) {
        super(output);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        super.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        super.write(bytes, offset, length);
    }

    public void writeString(String value) throws IOException {
        super.write(DUUIBytes.toUtf8Bytes(value));
    }
}

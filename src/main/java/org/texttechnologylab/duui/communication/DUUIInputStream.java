package org.texttechnologylab.duui.communication;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class DUUIInputStream extends FilterInputStream {
    public DUUIInputStream(InputStream input) {
        super(input);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return super.readAllBytes();
    }
}

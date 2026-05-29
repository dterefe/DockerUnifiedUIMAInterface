package org.texttechnologylab.duui.clients.handle;

import java.nio.file.Path;

public record DUUIPath(String value) {
    public DUUIPath {
        Path.of(value);
    }

    public DUUIAddress address() {
        return new DUUIAddress("file", null, Path.of(value).toAbsolutePath().toString(), null, null);
    }
}

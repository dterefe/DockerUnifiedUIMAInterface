package org.texttechnologylab.duui.clients.handle;

import java.net.URI;

public record DUUIUri(String value) {
    public DUUIUri {
        URI.create(value);
    }

    public DUUIAddress address() {
        return DUUIAddress.parse(value);
    }
}

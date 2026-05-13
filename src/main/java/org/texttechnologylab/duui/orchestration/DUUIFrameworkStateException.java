package org.texttechnologylab.duui.orchestration;

public final class DUUIFrameworkStateException extends IllegalStateException {
    public DUUIFrameworkStateException(String message) {
        super(message);
    }

    public DUUIFrameworkStateException(String message, Throwable cause) {
        super(message, cause);
    }
}

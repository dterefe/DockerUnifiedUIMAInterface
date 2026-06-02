package org.texttechnologylab.duui.exception;

import java.io.IOException;

public class DUUICancellationException extends IOException {
    public DUUICancellationException(String message) {
        super(message);
    }

    public DUUICancellationException(Throwable cause) {
        super(message(cause), cause);
    }

    public DUUICancellationException(String message, Throwable cause) {
        super(message, cause);
    }

    private static String message(Throwable cause) {
        if (cause == null) {
            return "DUUI operation cancelled";
        }
        String message = cause.getMessage();
        return message == null || message.isBlank()
                ? "DUUI operation cancelled: " + cause.getClass().getName()
                : "DUUI operation cancelled: " + message;
    }
}

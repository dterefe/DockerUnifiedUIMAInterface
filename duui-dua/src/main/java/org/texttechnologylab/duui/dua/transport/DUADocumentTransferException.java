package org.texttechnologylab.duui.dua.transport;

public class DUADocumentTransferException extends RuntimeException {
    public DUADocumentTransferException(String message) {
        super(message);
    }

    public DUADocumentTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}

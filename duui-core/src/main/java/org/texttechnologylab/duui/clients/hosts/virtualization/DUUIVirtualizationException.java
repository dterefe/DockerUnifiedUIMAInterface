package org.texttechnologylab.duui.clients.hosts.virtualization;

public sealed abstract class DUUIVirtualizationException extends Exception
    permits DUUIContainerBuildException,
    DUUIContainerDeleteException,
    DUUIContainerImageException,
    DUUIContainerInspectException,
    DUUIContainerRunException,
    DUUIContainerStartException,
    DUUIContainerStopException,
    DUUIVirtualizationConnectionException,
    DUUIVirtualizationUnsupportedOperationException {
    protected DUUIVirtualizationException(String message) {
        super(message);
    }

    protected DUUIVirtualizationException(String message, Throwable cause) {
        super(message, cause);
    }
}

package org.texttechnologylab.duui.dua.transport;

public record DUATransportJob(String jobId,
                              DUATransportJobStatus status,
                              String operation,
                              int documentCount,
                              String source,
                              String target,
                              String message,
                              long createdEpochMs,
                              long finishedEpochMs) {
    public DUATransportJob {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId must not be blank");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (operation == null || operation.isBlank()) {
            throw new IllegalArgumentException("operation must not be blank");
        }
        if (documentCount < 0) {
            throw new IllegalArgumentException("documentCount must not be negative");
        }
    }

    public DUATransportJob running() {
        return new DUATransportJob(jobId, DUATransportJobStatus.RUNNING, operation, documentCount,
                source, target, message, createdEpochMs, 0);
    }

    public DUATransportJob succeeded(int count, String successMessage) {
        return new DUATransportJob(jobId, DUATransportJobStatus.SUCCEEDED, operation, count,
                source, target, successMessage, createdEpochMs, System.currentTimeMillis());
    }

    public DUATransportJob failed(String failureMessage) {
        return new DUATransportJob(jobId, DUATransportJobStatus.FAILED, operation, documentCount,
                source, target, failureMessage, createdEpochMs, System.currentTimeMillis());
    }
}

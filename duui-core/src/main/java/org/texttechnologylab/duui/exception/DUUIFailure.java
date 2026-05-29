package org.texttechnologylab.duui.exception;

import java.time.Instant;

public final class DUUIFailure {
    private final DUUIFailureCategory category;
    private final DUUIFailureSeverity severity;
    private final DUUIRecoverability recoverability;
    private final DUUIFailureAction recommendedAction;
    private final String artifactId;
    private final String payloadType;
    private final String checkpointId;
    private final String stageId;
    private final String componentId;
    private final String nodeId;
    private final int attempt;
    private final String message;
    private final Throwable cause;
    private final Instant createdAt;

    public DUUIFailure(
            DUUIFailureCategory category,
            DUUIFailureSeverity severity,
            DUUIRecoverability recoverability,
            DUUIFailureAction recommendedAction,
            String artifactId,
            String payloadType,
            String checkpointId,
            String stageId,
            String componentId,
            String nodeId,
            int attempt,
            String message,
            Throwable cause
    ) {
        this.category = category == null ? DUUIFailureCategory.PROGRAMMING_BUG : category;
        this.severity = severity == null ? DUUIFailureSeverity.ERROR : severity;
        this.recoverability = recoverability == null ? DUUIRecoverability.NON_RETRYABLE : recoverability;
        this.recommendedAction = recommendedAction == null ? DUUIFailureAction.FAIL_FAST : recommendedAction;
        this.artifactId = artifactId;
        this.payloadType = payloadType;
        this.checkpointId = checkpointId;
        this.stageId = stageId;
        this.componentId = componentId;
        this.nodeId = nodeId;
        this.attempt = attempt;
        this.message = message;
        this.cause = cause;
        this.createdAt = Instant.now();
    }

    public DUUIFailureCategory category() { return category; }
    public DUUIFailureSeverity severity() { return severity; }
    public DUUIRecoverability recoverability() { return recoverability; }
    public DUUIFailureAction recommendedAction() { return recommendedAction; }
    public String artifactId() { return artifactId; }
    public String payloadType() { return payloadType; }
    public String checkpointId() { return checkpointId; }
    public String stageId() { return stageId; }
    public String componentId() { return componentId; }
    public String nodeId() { return nodeId; }
    public int attempt() { return attempt; }
    public String message() { return message; }
    public Throwable cause() { return cause; }
    public Instant createdAt() { return createdAt; }
}

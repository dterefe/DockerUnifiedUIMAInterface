package org.texttechnologylab.duui.exception;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.orchestration.DUUINode;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

public final class DUUIFailureClassifier {
    public DUUIFailure classify(Throwable throwable, DUUIArtifact<?> artifact, DUUICheckpoint<?> checkpoint, DUUIStage<?> stage, DUUINode node) {
        Throwable root = rootCause(throwable);
        String message = throwable == null ? null : throwable.getMessage();
        String lower = ((root == null || root.getMessage() == null) ? "" : root.getMessage()).toLowerCase();

        DUUIFailureCategory category = DUUIFailureCategory.PROGRAMMING_BUG;
        DUUIFailureSeverity severity = DUUIFailureSeverity.FATAL;
        DUUIRecoverability recoverability = DUUIRecoverability.NON_RETRYABLE;
        DUUIFailureAction action = DUUIFailureAction.FAIL_FAST;

        if (root instanceof CancellationException || Thread.currentThread().isInterrupted()) {
            category = DUUIFailureCategory.CANCELLATION;
            severity = DUUIFailureSeverity.WARNING;
            recoverability = DUUIRecoverability.RESUMABLE;
            action = DUUIFailureAction.CHECKPOINT_AND_STOP;
        } else if (root instanceof TimeoutException || root instanceof SocketTimeoutException || root instanceof SQLTimeoutException) {
            category = DUUIFailureCategory.TIMEOUT;
            severity = DUUIFailureSeverity.ERROR;
            recoverability = DUUIRecoverability.RETRYABLE_WITH_BACKOFF;
            action = DUUIFailureAction.BACKOFF_AND_RETRY;
        } else if (root instanceof SQLTransientConnectionException || lower.contains("connection pool") || lower.contains("connection is not available")) {
            category = DUUIFailureCategory.RESOURCE_EXHAUSTION;
            severity = DUUIFailureSeverity.ERROR;
            recoverability = DUUIRecoverability.RETRYABLE_AFTER_THROTTLE;
            action = DUUIFailureAction.THROTTLE_AND_RETRY;
        } else if (root instanceof SQLTransientException || root instanceof ConnectException || lower.contains("deadlock") || lower.contains("lock timeout")) {
            category = DUUIFailureCategory.TRANSIENT_INFRASTRUCTURE;
            severity = DUUIFailureSeverity.ERROR;
            recoverability = DUUIRecoverability.RETRYABLE_WITH_BACKOFF;
            action = DUUIFailureAction.BACKOFF_AND_RETRY;
        }

        return new DUUIFailure(
                category,
                severity,
                recoverability,
                action,
                artifact == null ? null : artifact.id(),
                artifact == null ? null : artifact.artifactType().id(),
                checkpoint == null ? null : checkpoint.id(),
                stage == null ? null : stage.id(),
                stage == null ? null : stage.componentId(),
                node == null ? null : node.id(),
                artifact == null ? 0 : artifact.state().attempt(),
                message,
                throwable
        );
    }

    private static Throwable rootCause(Throwable throwable) {
        if (throwable == null) return null;
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) current = current.getCause();
        return current;
    }
}

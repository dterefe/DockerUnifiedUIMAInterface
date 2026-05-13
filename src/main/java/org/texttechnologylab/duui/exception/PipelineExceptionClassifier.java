package org.texttechnologylab.duui.exception;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLTransientException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

public final class PipelineExceptionClassifier {
    private PipelineExceptionClassifier() {}

    public static PipelineException classify(
            String message,
            Throwable throwable,
            FailureScope scope,
            PipelineFailureContext context
    ) {
        Throwable root = rootCause(throwable);
        String lowerMessage = ((root == null ? "" : root.getMessage()) + " " + message).toLowerCase();

        if (throwable instanceof PipelineException pipelineException) {
            return pipelineException;
        }
        if (root instanceof CancellationException || Thread.currentThread().isInterrupted()) {
            return new CancellationFailureException(message, throwable, scope, context);
        }
        if (root instanceof TimeoutException || root instanceof SocketTimeoutException || root instanceof SQLTimeoutException) {
            return new TimeoutFailureException(message, throwable, scope, context);
        }
        if (root instanceof SQLTransientConnectionException || lowerMessage.contains("connection is not available") || lowerMessage.contains("connection pool")) {
            return new ResourceExhaustionFailureException(message, throwable, scope, context);
        }
        if (root instanceof SQLTransientException || root instanceof ConnectException || lowerMessage.contains("deadlock") || lowerMessage.contains("lock timeout")) {
            return new TransientInfrastructureFailureException(message, throwable, scope, context);
        }
        if (root instanceof NullPointerException || root instanceof ClassCastException || root instanceof IllegalStateException) {
            return new ProgrammingBugFailureException(message, throwable, scope, context);
        }
        return new ProgrammingBugFailureException(message, throwable, scope, context);
    }

    private static Throwable rootCause(Throwable throwable) {
        if (throwable == null) return null;
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}

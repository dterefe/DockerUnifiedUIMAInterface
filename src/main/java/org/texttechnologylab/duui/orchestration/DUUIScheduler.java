package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.pipeline.DUUIExecutor;

public interface DUUIScheduler {
    <T> DUUIExecutionResult<T> schedule(DUUITask<DUUIExecutionResult<T>> task, DUUIExecutor executor);
}

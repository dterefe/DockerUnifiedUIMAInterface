package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.exception.DUUIExecutionResult;
import org.texttechnologylab.duui.pipeline.DUUIExecutor;

public final class DUUISequentialScheduler implements DUUIScheduler {
    @Override
    public <T> DUUIExecutionResult<T> schedule(DUUITask<DUUIExecutionResult<T>> task, DUUIExecutor executor) {
        executor.runInline(task);
        return task.await();
    }
}

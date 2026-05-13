package org.texttechnologylab.duui.orchestration;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class DUUIPlatformExecutorService extends ThreadPoolExecutor {
    public DUUIPlatformExecutorService(String orchestratorId, int parallelism) {
        super(
                Math.max(1, parallelism),
                Math.max(1, parallelism),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                DUUIFactory.platformThreadFactory(orchestratorId)
        );
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        if (runnable instanceof DUUITask<?> task) {
            DUUIWorker.current().bind(new DUUIWorker.DUITaskBinding(task));
        }
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        try {
            if (runnable instanceof DUUITask<?> task && DUUIWorker.current().currentTask() == task) {
                DUUIWorker.current().clear(new DUUIWorker.DUITaskBinding(task));
            }
        } finally {
            super.afterExecute(runnable, throwable);
        }
    }
}

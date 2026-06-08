package org.texttechnologylab.duui.orchestration.worker;

import org.texttechnologylab.duui.DUUIWorkerContext;
import org.texttechnologylab.duui.orchestration.DUUITask;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class DUUIPlatformExecutor extends ThreadPoolExecutor {

    public DUUIPlatformExecutor(String orchestratorId, DUUIWorker.Type type, int parallelism) {
        super(
                Math.max(1, parallelism),
                Math.max(1, parallelism),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                DUUIWorker.Factory.platform(orchestratorId, type)
        );
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        DUUIWorkerContext parentContext = captureParentContext();
        if (parentContext != null) {
            DUUIWorkerContext.current().copyFrom(parentContext);
        }
        if (runnable instanceof DUUITask<?> task) {
            DUUIWorker.current().bind(task);
        }
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        try {
            if (runnable instanceof DUUITask<?> task && DUUIWorker.current().currentTask() == task) {
                DUUIWorker.current().clear(task);
            }
        } finally {
            super.afterExecute(runnable, throwable);
        }
    }

    private static DUUIWorkerContext captureParentContext() {
        try {
            return DUUIWorkerContext.current().copy();
        } catch (Exception ignored) {
            return null;
        }
    }
}

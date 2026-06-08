package org.texttechnologylab.duui.orchestration.worker;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class DUUIVirtualExecutorService extends AbstractExecutorService {
    private final String orchestratorId;
    private final DUUIWorker.Factory factory;
    private final Collection<Thread> threads = ConcurrentHashMap.newKeySet();
    private volatile boolean shutdown;

    public DUUIVirtualExecutorService(String orchestratorId, DUUIWorker.Type type) {
        this.orchestratorId = orchestratorId;
        this.factory = DUUIWorker.Factory.virtual(orchestratorId, type);
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && threads.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (!isTerminated() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        return isTerminated();
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown) throw new IllegalStateException("DUUI virtual executor is shut down.");
        Thread thread = factory.newThread(() -> {
            try {
                command.run();
            } finally {
                threads.remove(Thread.currentThread());
            }
        });
        threads.add(thread);
        thread.start();
    }
}

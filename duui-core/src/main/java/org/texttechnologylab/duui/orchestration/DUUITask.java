package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.event.DUUIEventContext;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DUUITask<T> implements Runnable, Future<T>, AutoCloseable {
    private final String id;
    private final String orchestratorId;
    private final DUUIExecutionContext context;
    private final Callable<T> work;
    private final CompletableFuture<T> completion = new CompletableFuture<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile Future<?> submittedFuture;
    private volatile DUUIDispatchMode phaseDispatchOverride;

    public DUUITask(String orchestratorId, DUUIExecutionContext context, Callable<T> work) {
        this.id = UUID.randomUUID().toString();
        this.orchestratorId = Objects.requireNonNull(orchestratorId, "orchestratorId");
        this.context = context == null ? new DUUIExecutionContext() : context;
        this.work = Objects.requireNonNull(work, "work");
        if (this.context.eventContext() == null) {
            this.context.eventContext(DUUIEventContext.root(this.orchestratorId, this.id));
        } else {
            this.context.eventContext(this.context.eventContext().toBuilder()
                    .orchestratorId(this.orchestratorId)
                    .taskId(this.id)
                    .build());
        }
    }

    public DUUITask<T> submit(org.texttechnologylab.duui.orchestration.worker.DUUIExecutor executor) {
        this.submittedFuture = Objects.requireNonNull(executor, "executor").submit(this);
        return this;
    }

    public void phaseDispatchOverride(DUUIDispatchMode mode) {
        this.phaseDispatchOverride = mode;
    }

    public T await() {
        try {
            return completion.join();
        } catch (CompletionException e) {
            throw e;
        }
    }

    public T await(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return completion.get(timeout, unit);
        } catch (TimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    public boolean cancel() {
        return completion.cancel(true);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        Future<?> future = submittedFuture;
        if (future != null && future != this) future.cancel(mayInterruptIfRunning);
        return completion.cancel(mayInterruptIfRunning);
    }

    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        DUUIWorker worker = DUUIWorker.current();
        context.eventContext(context.eventContext().toBuilder().workerId(worker.id()).build());
        try (DUUIDispatcher.DispatchOverride ignoredDispatch = DUUIDispatcher.bindDispatchOverride(phaseDispatchOverride)) {
            if (worker.currentTask() == this) {
                try {
                    completion.complete(work.call());
                } catch (Throwable t) {
                    completion.completeExceptionally(t);
                }
                return;
            }
            try (DUUITaskScope ignored = enter()) {
                completion.complete(work.call());
            } catch (Throwable t) {
                completion.completeExceptionally(t);
            }
        }
    }

    public DUUITaskScope enter() {
        return new DUUITaskScope(this);
    }

    @Override
    public void close() {
        cancel(true);
    }

    public String id() { return id; }
    public String orchestratorId() { return orchestratorId; }
    public DUUIExecutionContext context() { return context; }
    @Override
    public boolean isDone() { return completion.isDone(); }
    @Override
    public boolean isCancelled() { return completion.isCancelled(); }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return completion.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return completion.get(timeout, unit);
    }
}

package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.*;

/**
 * VirtualTasks provides a simple, declarative API for scheduling and executing small tasks
 * on Java 21 virtual threads, with per-instance queuing, exception handling,
 * completion waiting via CountDownLatch, and callback support via the inner Task<V> class.
 */
public class TaskScheduler<T> {
    // Per-instance virtual-thread-per-task executor
    private final ExecutorService executor;

    // Dispatcher virtual thread reference
    private final Thread dispatcherThread;

    // FIFO queue for tasks; dispatcher thread submits each to executor
    private final BlockingQueue<Task<T>> taskQueue;

    // Documentation list of all tasks submitted or chained
    private final List<Task<T>> tasks;

    // Latch to await completion of a fixed number of tasks
    private final CountDownLatch latch;

    /**
     * Creates a new VirtualTasks instance with no awaitable latch (awaitCompletion is no-op).
     */
    public TaskScheduler() {
        this(0);
    }

    /**
     * Creates a new VirtualTasks instance with a CountDownLatch for the given taskCount.
     * If taskCount < 1, latch is disabled and awaitCompletion() will return immediately.
     * @param taskCount number of tasks to await
     */
    public TaskScheduler(int taskCount) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.taskQueue = new LinkedBlockingQueue<>();
        this.tasks = new CopyOnWriteArrayList<>();
        this.latch = taskCount > 0 ? new CountDownLatch(taskCount) : null;

        // Dispatcher runs in its own virtual thread
        this.dispatcherThread = Thread.startVirtualThread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Task<T> task = taskQueue.take();
                    task.scheduleOnExecutor();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Bootstrap a pipeline: creates a new instance and submits each Callable,
     * populating the documentation list and queue.
     */
    @SafeVarargs
    public static <T> TaskScheduler<T> of(Callable<T>... callables) {
        TaskScheduler<T> vt = new TaskScheduler<>();
        for (Callable<T> c : callables) {
            vt.submit(c);
        }
        return vt;
    }

    /**
     * Submit a standalone task: enqueue it, add to docs list, and return its Task<V>.
     */
    public Task<T> submit(Callable<T> callable) {
        Task<T> task = new Task<>(callable);
        tasks.add(task);
        taskQueue.add(task);
        return task;
    }

    /**
     * Transform each task's result; mapping tasks are enqueued on this instance.
     */
    public <R> TaskScheduler<R> map(Function<? super T, ? extends R> mapper) {
        TaskScheduler<R> vt = new TaskScheduler<>(latchCount());
        for (Task<T> t : tasks) {
            vt.submit(() -> {
                T res = t.get();
                return mapper.apply(res);
            });
        }
        return vt;
    }

    /**
     * Filter task results; filtering tasks are enqueued on this instance.
     */
    public TaskScheduler<T> filter(Predicate<? super T> predicate) {
        TaskScheduler<T> vt = new TaskScheduler<>(latchCount());
        for (Task<T> t : tasks) {
            vt.submit(() -> {
                T res = t.get();
                return predicate.test(res) ? res : null;
            });
        }
        return vt;
    }

    /**
     * Returns an unmodifiable view of the tasks list (for documentation or chaining).
     */
    public List<Task<T>> tasks() {
        return Collections.unmodifiableList(tasks);
    }

    /**
     * Waits until queue is drained and all tasks complete via CountDownLatch.
     * If latch was not initialized (taskCount <1), returns immediately.
     */
    public void awaitCompletion() throws InterruptedException {
        if (latch == null) {
            return; // no awaitable tasks defined
        }
        // wait until queue is empty
        while (!taskQueue.isEmpty()) {
            Thread.sleep(50);
        }
        // wait for all tasks to complete
        latch.await();
    }

    /**
     * Interrupts the dispatcher and stops executing queued or running tasks.
     */
    public void interruptAll() {
        dispatcherThread.interrupt();
        executor.shutdownNow();
        taskQueue.clear();
    }

    /**
     * Shut down this instance's executor gracefully when finished.
     */
    public void shutdown() {
        executor.shutdown();
    }

    /**
     * Returns the original latch count or 0 if none.
     */
    private int latchCount() {
        return latch == null ? 0 : (int) latch.getCount();
    }

    /**
     * Represents a queued task executing on a virtual thread, capturing status,
     * result or exception, and supporting callback chaining via thenAccept/exceptionally.
     */
    public class Task<V> {
        private final Callable<V> callable;
        private volatile Future<V> future;
        private volatile V result;
        private volatile Throwable exception;
        private final List<Consumer<? super V>> acceptHandlers = new CopyOnWriteArrayList<>();
        private final List<Consumer<? super Throwable>> exceptionHandlers = new CopyOnWriteArrayList<>();

        private Task(Callable<V> callable) {
            this.callable = callable;
        }

        // Internal: dispatch this task to the executor
        private void scheduleOnExecutor() {
            future = executor.submit(() -> {
                try {
                    V res = callable.call();
                    this.result = res;
                    acceptHandlers.forEach(h -> h.accept(res));
                    return res;
                } catch (Throwable ex) {
                    this.exception = ex;
                    exceptionHandlers.forEach(h -> h.accept(ex));
                    throw ex;
                } finally {
                    // count down latch if present
                    if (latch != null) {
                        latch.countDown();
                    }
                }
            });
        }

        /**
         * Chain a callback to process the successful result.
         */
        public Task<V> thenAccept(Consumer<? super V> handler) {
            acceptHandlers.add(handler);
            if (status() == TaskStatus.COMPLETED) handler.accept(result);
            return this;
        }

        /**
         * Chain a callback to handle any exception.
         */
        public Task<V> exceptionally(Consumer<? super Throwable> handler) {
            exceptionHandlers.add(handler);
            if (status() == TaskStatus.FAILED) handler.accept(exception);
            return this;
        }

        /**
         * Block until done, returning result or throwing exception.
         */
        public V get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        /**
         * Non-blocking exception retrieval.
         */
        public Throwable exception() {
            return exception;
        }

        /**
         * Current task status: PENDING, COMPLETED, or FAILED.
         */
        public TaskStatus status() {
            if (future == null || !future.isDone()) return TaskStatus.PENDING;
            return exception != null ? TaskStatus.FAILED : TaskStatus.COMPLETED;
        }
    }

    /** Task status enumeration. */
    public enum TaskStatus {
        PENDING,
        COMPLETED,
        FAILED
    }
}

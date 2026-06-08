package org.texttechnologylab.duui;

import org.texttechnologylab.duui.ems.GID;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic queue wrapper with telemetry.
 * Wraps any {@link BlockingQueue} and tracks depth, throughput, and wait times.
 * Replaces all raw {@code BlockingQueue} and {@code ConcurrentLinkedQueue} usage.
 *
 * <p>Tagged with GID+Name for identification and traceable telemetry.</p>
 *
 * [DESIGN: lines 199-225, 286]
 *
 * @param <T> element type
 */
public final class DUUIPool<T> {

    private final GID gid;
    private final String name;
    private final BlockingQueue<T> queue;
    private final AtomicLong offerCount = new AtomicLong();
    private final AtomicLong takeCount = new AtomicLong();
    private final AtomicLong totalWaitNanos = new AtomicLong();
    private final AtomicLong lastOfferNanos = new AtomicLong();
    private final AtomicLong firstOfferNanos = new AtomicLong(Long.MAX_VALUE);

    public DUUIPool(GID gid, String name, BlockingQueue<T> queue) {
        this.gid = Objects.requireNonNull(gid, "gid");
        this.name = Objects.requireNonNull(name, "name");
        this.queue = Objects.requireNonNull(queue, "queue");
    }

    /**
     * Insert the specified element into this pool if possible.
     *
     * @param item the element to add
     * @return {@code true} if the element was added
     */
    public boolean offer(T item) {
        long now = System.nanoTime();
        boolean accepted = queue.offer(item);
        if (accepted) {
            offerCount.incrementAndGet();
            lastOfferNanos.set(now);
            firstOfferNanos.accumulateAndGet(now, Math::min);
        }
        return accepted;
    }

    /**
     * Retrieve and remove the head of the pool, waiting if necessary.
     *
     * @return the head element
     * @throws InterruptedException if interrupted while waiting
     */
    public T take() throws InterruptedException {
        long start = System.nanoTime();
        T item = queue.take();
        totalWaitNanos.addAndGet(System.nanoTime() - start);
        takeCount.incrementAndGet();
        return item;
    }

    /**
     * Retrieve and remove the head of the pool, or return {@code null} if empty.
     *
     * @return the head element, or {@code null} if empty
     */
    public T poll() {
        T item = queue.poll();
        if (item != null) {
            takeCount.incrementAndGet();
        }
        return item;
    }

    /**
     * Current number of elements in the pool.
     *
     * @return queue depth
     */
    public int depth() {
        return queue.size();
    }

    /**
     * Approximate take throughput in items per second.
     *
     * @return items/second based on cumulative take count and wait time
     */
    public double throughput() {
        long nanos = totalWaitNanos.get();
        long takes = takeCount.get();
        if (nanos == 0 || takes == 0) {
            return 0.0;
        }
        return (double) takes / (nanos / 1_000_000_000.0);
    }

    /**
     * NanoTime of the most recent successful offer.
     *
     * @return nanoseconds timestamp of last offer
     */
    public long lastOfferNanos() {
        return lastOfferNanos.get();
    }

    /**
     * NanoTime of the earliest successful offer since pool creation.
     *
     * @return nanoseconds timestamp of first offer
     */
    public long firstOfferNanos() {
        return firstOfferNanos.get();
    }

    public GID gid() {
        return gid;
    }

    public String name() {
        return name;
    }

    /**
     * Expose the underlying queue for operations that require direct access
     * (e.g., draining to a collection).
     *
     * @return the wrapped blocking queue
     */
    public BlockingQueue<T> queue() {
        return queue;
    }
}

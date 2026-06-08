package org.texttechnologylab.duui.orchestration.scheduling;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler policy interface for checkpoint selection.
 * Implementations define the strategy for picking the next artifact from available checkpoints.
 *
 * [DESIGN: lines 114-136]
 */
public interface DUUISchedulerPolicy {
    Selection select(Snapshot snapshot);

    /** Iterate checkpoints in order, pick first with available artifact. */
    static DUUISchedulerPolicy firstReady() {
        return new FirstReady();
    }

    /** Pick a random checkpoint that has available artifacts. */
    static DUUISchedulerPolicy random() {
        return new RandomPolicy();
    }

    /** Cycle through checkpoints in round-robin order. */
    static DUUISchedulerPolicy roundRobin() {
        return new RoundRobin();
    }

    /** Pick the checkpoint that most recently received an artifact. */
    static DUUISchedulerPolicy lastComesFirst() {
        return new LastComesFirst();
    }

    /** Pick the checkpoint whose oldest artifact has been waiting longest. */
    static DUUISchedulerPolicy pollFirst() {
        return new PollFirst();
    }

    record Snapshot(
            Map<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> pools,
            int inFlight,
            DUUIExecutor executor
    ) {
        public Snapshot {
            pools = pools == null ? Map.of() : pools;
        }
    }

    record Selection(DUUICheckpoint<?> checkpoint, DUUIArtifact<?> artifact) {
        public Selection {
            Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(artifact, "artifact");
        }
    }

    /**
     * Iterate checkpoints in insertion order, pick the first with a non-empty pool.
     */
    final class FirstReady implements DUUISchedulerPolicy {
        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.pools().isEmpty()) {
                return null;
            }
            for (Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> entry : snapshot.pools().entrySet()) {
                DUUIPool<DUUIArtifact<?>> pool = entry.getValue();
                if (pool == null || pool.depth() == 0) {
                    continue;
                }
                DUUIArtifact<?> artifact = pool.poll();
                if (artifact == null) {
                    continue;
                }
                DUUIExecutor executor = snapshot.executor();
                if (executor != null && !DUUIScheduler.canDispatch(snapshot.inFlight(), executor.dispatchPolicyFor(entry.getKey(), artifact))) {
                    pool.offer(artifact); // return to pool if can't dispatch
                    continue;
                }
                return new Selection(entry.getKey(), artifact);
            }
            return null;
        }
    }

    /**
     * Pick a random checkpoint from those that have available artifacts.
     */
    final class RandomPolicy implements DUUISchedulerPolicy {
        private final Random rng = new Random();

        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.pools().isEmpty()) {
                return null;
            }
            List<Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>>> candidates = new ArrayList<>();
            for (Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> entry : snapshot.pools().entrySet()) {
                if (entry.getValue() != null && entry.getValue().depth() > 0) {
                    candidates.add(entry);
                }
            }
            if (candidates.isEmpty()) {
                return null;
            }
            Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> chosen = candidates.get(rng.nextInt(candidates.size()));
            DUUIArtifact<?> artifact = chosen.getValue().poll();
            if (artifact == null) {
                return null;
            }
            return new Selection(chosen.getKey(), artifact);
        }
    }

    /**
     * Cycle through checkpoints in round-robin order across select calls.
     */
    final class RoundRobin implements DUUISchedulerPolicy {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.pools().isEmpty()) {
                return null;
            }
            List<Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>>> entries = new ArrayList<>(snapshot.pools().entrySet());
            if (entries.isEmpty()) {
                return null;
            }
            for (int attempt = 0; attempt < entries.size(); attempt++) {
                int idx = Math.abs(counter.getAndIncrement()) % entries.size();
                Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> entry = entries.get(idx);
                DUUIPool<DUUIArtifact<?>> pool = entry.getValue();
                if (pool != null && pool.depth() > 0) {
                    DUUIArtifact<?> artifact = pool.poll();
                    if (artifact != null) {
                        return new Selection(entry.getKey(), artifact);
                    }
                }
            }
            return null;
        }
    }

    /**
     * Pick the checkpoint whose most recent offer was newest (highest offer timestamp wins).
     * Falls back to first ready if no timestamps differ.
     */
    final class LastComesFirst implements DUUISchedulerPolicy {
        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.pools().isEmpty()) {
                return null;
            }
            Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> best = null;
            long bestTimestamp = Long.MIN_VALUE;

            for (Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> entry : snapshot.pools().entrySet()) {
                DUUIPool<DUUIArtifact<?>> pool = entry.getValue();
                if (pool == null || pool.depth() == 0) {
                    continue;
                }
                long lastOfferNanos = pool.lastOfferNanos();
                if (lastOfferNanos > bestTimestamp) {
                    bestTimestamp = lastOfferNanos;
                    best = entry;
                }
            }
            if (best == null) {
                return null;
            }
            DUUIArtifact<?> artifact = best.getValue().poll();
            if (artifact == null) {
                return null;
            }
            return new Selection(best.getKey(), artifact);
        }
    }

    /**
     * Pick the checkpoint whose oldest artifact has been waiting longest.
     * Uses take-based timing: the pool with the longest cumulative wait has priority.
     */
    final class PollFirst implements DUUISchedulerPolicy {
        @Override
        public Selection select(Snapshot snapshot) {
            if (snapshot == null || snapshot.pools().isEmpty()) {
                return null;
            }
            Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> best = null;
            long oldestNanos = Long.MAX_VALUE;

            for (Map.Entry<DUUICheckpoint<?>, DUUIPool<DUUIArtifact<?>>> entry : snapshot.pools().entrySet()) {
                DUUIPool<DUUIArtifact<?>> pool = entry.getValue();
                if (pool == null || pool.depth() == 0) {
                    continue;
                }
                long firstOfferNanos = pool.firstOfferNanos();
                if (firstOfferNanos < oldestNanos) {
                    oldestNanos = firstOfferNanos;
                    best = entry;
                }
            }
            if (best == null) {
                return null;
            }
            DUUIArtifact<?> artifact = best.getValue().poll();
            if (artifact == null) {
                return null;
            }
            return new Selection(best.getKey(), artifact);
        }
    }
}

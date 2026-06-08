package org.texttechnologylab.duui.dua.store.memory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.store.DUAAnnotationIndex;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

/**
 * An in-memory annotation index backed by an interval tree for O(log n) range queries.
 * <p>
 * Modifications are batched: writes are recorded in pending lists and the tree
 * is rebuilt after {@link #REBUILD_THRESHOLD} accumulated changes. Queries
 * transparently incorporate pending modifications by filtering out removed
 * spans and merging in added spans.
 */
public final class DUAMemoryAnnotationIndex implements DUAAnnotationIndex {

    /** Root of the interval tree (volatile for lock-free reading after rebuild). */
    private volatile Node root;

    /** Read/write lock protecting concurrent access to the tree and pending lists. */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // ── Pending modifications (batch rebuild) ──────────────────────────────

    private final List<DUAAnnotationSpan> pendingAdds = new ArrayList<>();
    private final List<Long> pendingRemoves = new ArrayList<>();

    /** Max accumulated modifications before automatic tree rebuild. */
    static final int REBUILD_THRESHOLD = 100;

    /** Approximate total number of spans (used for revision tracking). */
    private volatile int totalSpans = 0;

    // ── Interval tree node ────────────────────────────────────────────────

    static final class Node {
        /** Median begin value used for partitioning. */
        final int center;
        /** Centre-list spans sorted by begin ascending, then end ascending. */
        final List<DUAAnnotationSpan> byBegin;
        /** Centre-list spans sorted by end descending, then begin ascending. */
        final List<DUAAnnotationSpan> byEnd;
        /** Left subtree (spans that end before {@code center}). */
        final Node left;
        /** Right subtree (spans that start after {@code center}). */
        final Node right;

        Node(final int center,
             final List<DUAAnnotationSpan> byBegin,
             final List<DUAAnnotationSpan> byEnd,
             final Node left,
             final Node right) {
            this.center = center;
            this.byBegin = byBegin;
            this.byEnd = byEnd;
            this.left = left;
            this.right = right;
        }
    }

    // ── Write operations ──────────────────────────────────────────────────

    @Override
    public DUAWriteResult index(final DUAAnnotationSpan span) {
        Objects.requireNonNull(span, "span");
        rwLock.writeLock().lock();
        try {
            pendingAdds.add(span);
            totalSpans++;
            if (pendingAdds.size() + pendingRemoves.size() >= REBUILD_THRESHOLD) {
                rebuildTree();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
        return new DUAWriteResult(
                DUAId.of("sofa-" + span.sofaFsRef() + "#ann-" + span.fsRef()),
                new DUARevision(totalSpans));
    }

    /**
     * Removes the given span from the index (by fsRef).
     *
     * @param span the span to remove
     * @return write result descriptor
     */
    public DUAWriteResult remove(final DUAAnnotationSpan span) {
        Objects.requireNonNull(span, "span");
        rwLock.writeLock().lock();
        try {
            pendingRemoves.add(span.fsRef());
            totalSpans = Math.max(0, totalSpans - 1);
            if (pendingAdds.size() + pendingRemoves.size() >= REBUILD_THRESHOLD) {
                rebuildTree();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
        return new DUAWriteResult(
                DUAId.of("sofa-" + span.sofaFsRef() + "#ann-" + span.fsRef()),
                new DUARevision(totalSpans));
    }

    // ── Query ─────────────────────────────────────────────────────────────

    @Override
    public Stream<DUAAnnotationSpan> find(final DUAAnnotationSpanQuery query) {
        Objects.requireNonNull(query, "query");

        // Check whether a rebuild is needed under read lock; upgrade if so.
        rwLock.readLock().lock();
        try {
            if (pendingAdds.size() + pendingRemoves.size() >= REBUILD_THRESHOLD) {
                // Upgrade: release read, acquire write, rebuild, downgrade back to read.
                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    if (pendingAdds.size() + pendingRemoves.size() >= REBUILD_THRESHOLD) {
                        rebuildTree();
                    }
                    // Downgrade by acquiring read lock before releasing write
                    rwLock.readLock().lock();
                } finally {
                    rwLock.writeLock().unlock();
                    // read lock is now held
                }
            }

            // Determine the set of removed fsRefs for filtering
            final Set<Long> removed = new HashSet<>(pendingRemoves);
            final List<DUAAnnotationSpan> adds = new ArrayList<>(pendingAdds);

            return applyPending(
                    removed,
                    adds,
                    dispatchQuery(query));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // ── Query dispatch ────────────────────────────────────────────────────

    /**
     * Routes the query to the appropriate interval-tree traversal or fallback.
     */
    private Stream<DUAAnnotationSpan> dispatchQuery(final DUAAnnotationSpanQuery query) {
        return switch (query) {
            case DUAAnnotationSpanQuery.ExactSpan q      -> queryExact(q);
            case DUAAnnotationSpanQuery.CoveringPoint q  -> queryCoveringPoint(q);
            case DUAAnnotationSpanQuery.Overlapping q    -> queryOverlapping(q);
            case DUAAnnotationSpanQuery.ContainedIn q    -> queryContainedIn(q);
            case DUAAnnotationSpanQuery.CoveringSpan q   -> queryCoveringSpan(q);
            case DUAAnnotationSpanQuery.Neighborhood q   -> queryNeighborhood(q);
            case DUAAnnotationSpanQuery.SameSpanJoin q   -> querySameSpanJoin(q);
            case DUAAnnotationSpanQuery.RangeJoin q      -> queryRangeJoin(q);
            case DUAAnnotationSpanQuery.CoveredText q    -> queryCoveredText(q);
            case DUAAnnotationSpanQuery.Substring q      -> querySubstring(q);
            case DUAAnnotationSpanQuery.Pattern q        -> q.steps().stream()
                    .flatMap(this::find)
                    .distinct();
        };
    }

    // ── Interval-tree helpers ─────────────────────────────────────────────

    /** Checks whether the span matches the given sofa and (optional) type filter. */
    private static boolean matches(final DUAAnnotationSpan span,
                                   final long sofaFsRef,
                                   final OptionalInt typeId) {
        return span.sofaFsRef() == sofaFsRef
                && (typeId.isEmpty() || span.typeId() == typeId.getAsInt());
    }

    /** Filters a list by sofa + type and returns a stream. */
    private static Stream<DUAAnnotationSpan> filterList(final List<DUAAnnotationSpan> list,
                                                        final long sofaFsRef,
                                                        final OptionalInt typeId) {
        return list.stream()
                .filter(s -> matches(s, sofaFsRef, typeId));
    }

    // ── Overlapping query (core interval-tree traversal) ──────────────────

    /**
     * Finds all spans that overlap {@code [q.begin, q.end)}.
     */
    private Stream<DUAAnnotationSpan> queryOverlapping(
            final DUAAnnotationSpanQuery.Overlapping q) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectOverlapping(root, q.sofaFsRef(), q.begin(), q.end(), q.typeId(), result);
        }
        return result.stream();
    }

    private static void collectOverlapping(final Node node,
                                           final long sofa,
                                           final int qBegin,
                                           final int qEnd,
                                           final OptionalInt typeId,
                                           final List<DUAAnnotationSpan> result) {
        if (node == null) return;

        // If query end is left of center, only left subtree + crossing centre spans
        if (qEnd <= node.center) {
            // Centre spans that start before qEnd (byBegin is sorted by begin ASC)
            for (final DUAAnnotationSpan s : node.byBegin) {
                if (s.begin() >= qEnd) break; // remaining spans start too far right
                if (matches(s, sofa, typeId) && s.begin() < qEnd && qBegin < s.end()) {
                    result.add(s);
                }
            }
            collectOverlapping(node.left, sofa, qBegin, qEnd, typeId, result);
            return;
        }

        // If query begin is right of center, only right subtree + crossing centre spans
        if (qBegin >= node.center) {
            // Centre spans that end after qBegin (byEnd is sorted by end DESC)
            for (final DUAAnnotationSpan s : node.byEnd) {
                if (s.end() <= qBegin) break; // remaining spans end too far left
                if (matches(s, sofa, typeId) && s.begin() < qEnd && qBegin < s.end()) {
                    result.add(s);
                }
            }
            collectOverlapping(node.right, sofa, qBegin, qEnd, typeId, result);
            return;
        }

        // Query spans the centre: all centre spans overlap + recurse both sides
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (matches(s, sofa, typeId)) {
                result.add(s);
            }
        }
        collectOverlapping(node.left, sofa, qBegin, qEnd, typeId, result);
        collectOverlapping(node.right, sofa, qBegin, qEnd, typeId, result);
    }

    // ── Exact ─────────────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryExact(
            final DUAAnnotationSpanQuery.ExactSpan q) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectExact(root, q.sofaFsRef(), q.begin(), q.end(), q.typeId(), result);
        }
        return result.stream();
    }

    private static void collectExact(final Node node,
                                     final long sofa,
                                     final int qBegin,
                                     final int qEnd,
                                     final OptionalInt typeId,
                                     final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        // Exact spans must cross the centre or be entirely on one side
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (matches(s, sofa, typeId) && s.begin() == qBegin && s.end() == qEnd) {
                result.add(s);
            }
        }
        if (qEnd <= node.center) {
            collectExact(node.left, sofa, qBegin, qEnd, typeId, result);
        } else if (qBegin >= node.center) {
            collectExact(node.right, sofa, qBegin, qEnd, typeId, result);
        } else {
            collectExact(node.left, sofa, qBegin, qEnd, typeId, result);
            collectExact(node.right, sofa, qBegin, qEnd, typeId, result);
        }
    }

    // ── CoveringPoint ─────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryCoveringPoint(
            final DUAAnnotationSpanQuery.CoveringPoint q) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectCoveringPoint(root, q.sofaFsRef(), q.offset(), q.typeId(), result);
        }
        return result.stream();
    }

    private static void collectCoveringPoint(final Node node,
                                             final long sofa,
                                             final int offset,
                                             final OptionalInt typeId,
                                             final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        // Check centre-list spans: does any span cover the point?
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (s.begin() > offset) break; // spans sorted by begin ASC, past the point
            if (matches(s, sofa, typeId) && s.begin() <= offset && offset < s.end()) {
                result.add(s);
            }
        }
        if (offset < node.center) {
            collectCoveringPoint(node.left, sofa, offset, typeId, result);
        } else {
            collectCoveringPoint(node.right, sofa, offset, typeId, result);
        }
    }

    // ── ContainedIn ───────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryContainedIn(
            final DUAAnnotationSpanQuery.ContainedIn q) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectContainedIn(root, q.sofaFsRef(), q.begin(), q.end(), q.typeId(), result);
        }
        return result.stream();
    }

    private static void collectContainedIn(final Node node,
                                           final long sofa,
                                           final int qBegin,
                                           final int qEnd,
                                           final OptionalInt typeId,
                                           final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        // Centre spans whose begin is >= qBegin and end <= qEnd
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (s.begin() >= qEnd) break;
            if (matches(s, sofa, typeId)
                    && qBegin <= s.begin() && s.end() <= qEnd) {
                result.add(s);
            }
        }
        if (qEnd <= node.center) {
            collectContainedIn(node.left, sofa, qBegin, qEnd, typeId, result);
        } else if (qBegin >= node.center) {
            collectContainedIn(node.right, sofa, qBegin, qEnd, typeId, result);
        } else {
            collectContainedIn(node.left, sofa, qBegin, qEnd, typeId, result);
            collectContainedIn(node.right, sofa, qBegin, qEnd, typeId, result);
        }
    }

    // ── CoveringSpan ──────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryCoveringSpan(
            final DUAAnnotationSpanQuery.CoveringSpan q) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectCoveringSpan(root, q.sofaFsRef(), q.begin(), q.end(), q.typeId(), result);
        }
        return result.stream();
    }

    private static void collectCoveringSpan(final Node node,
                                            final long sofa,
                                            final int qBegin,
                                            final int qEnd,
                                            final OptionalInt typeId,
                                            final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        // Centre spans that contain [qBegin, qEnd)
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (s.begin() > qBegin) break; // begin > qBegin can't contain qBegin if it starts later... actually need to check more carefully
            if (matches(s, sofa, typeId)
                    && s.begin() <= qBegin && qEnd <= s.end()) {
                result.add(s);
            }
        }
        // Covering spans must start at or before qBegin, so they could be anywhere
        // Only recurse to subtrees that could contain spans starting <= qBegin
        if (qBegin < node.center) {
            collectCoveringSpan(node.left, sofa, qBegin, qEnd, typeId, result);
        }
        // Spans starting after qBegin can't contain qBegin, so no need to recurse right
        // unless qBegin >= node.center (then right spans could start at or before qBegin)
        if (qBegin >= node.center) {
            collectCoveringSpan(node.right, sofa, qBegin, qEnd, typeId, result);
        }
    }

    // ── Neighborhood ──────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryNeighborhood(
            final DUAAnnotationSpanQuery.Neighborhood q) {
        // We need all spans on the same sofa, sorted by begin, to locate the anchor.
        final List<DUAAnnotationSpan> scoped = collectAllOnSofa(q.sofaFsRef());
        int anchor = -1;
        for (int i = 0; i < scoped.size(); i++) {
            if (scoped.get(i).fsRef() == q.anchorFsRef()) {
                anchor = i;
                break;
            }
        }
        if (anchor < 0) {
            return Stream.empty();
        }
        final int from = Math.max(0, anchor - q.before());
        final int to = Math.min(scoped.size(), anchor + q.after() + 1);
        return scoped.subList(from, to).stream()
                .filter(s -> q.typeId().isEmpty() || s.typeId() == q.typeId().getAsInt());
    }

    /** Collects all spans on the given sofa, sorted by begin. */
    private List<DUAAnnotationSpan> collectAllOnSofa(final long sofaFsRef) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectAllOnSofa(root, sofaFsRef, result);
        }
        result.sort(Comparator.comparingInt(DUAAnnotationSpan::begin));
        return result;
    }

    private static void collectAllOnSofa(final Node node,
                                         final long sofa,
                                         final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (s.sofaFsRef() == sofa) {
                result.add(s);
            }
        }
        collectAllOnSofa(node.left, sofa, result);
        collectAllOnSofa(node.right, sofa, result);
    }

    // ── SameSpanJoin ──────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> querySameSpanJoin(
            final DUAAnnotationSpanQuery.SameSpanJoin q) {
        // Collect all left-type spans on this sofa, then find matches with right-type.
        final List<DUAAnnotationSpan> leftSpans = collectByTypeOnSofa(root, q.sofaFsRef(), q.leftTypeId());
        if (leftSpans.isEmpty()) return Stream.empty();
        final List<DUAAnnotationSpan> rightSpans = collectByTypeOnSofa(root, q.sofaFsRef(), q.rightTypeId());
        if (rightSpans.isEmpty()) return Stream.empty();

        // Build a set of (begin,end) pairs for fast lookup
        final Set<SpanKey> rightKeys = new HashSet<>();
        for (final DUAAnnotationSpan s : rightSpans) {
            rightKeys.add(new SpanKey(s.begin(), s.end()));
        }

        return leftSpans.stream()
                .filter(s -> rightKeys.contains(new SpanKey(s.begin(), s.end())));
    }

    private static List<DUAAnnotationSpan> collectByTypeOnSofa(final Node node,
                                                                final long sofa,
                                                                final int typeId) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        collectByTypeOnSofa(node, sofa, typeId, result);
        return result;
    }

    private static void collectByTypeOnSofa(final Node node,
                                            final long sofa,
                                            final int typeId,
                                            final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        for (final DUAAnnotationSpan s : node.byBegin) {
            if (s.sofaFsRef() == sofa && s.typeId() == typeId) {
                result.add(s);
            }
        }
        collectByTypeOnSofa(node.left, sofa, typeId, result);
        collectByTypeOnSofa(node.right, sofa, typeId, result);
    }

    // ── RangeJoin ─────────────────────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryRangeJoin(
            final DUAAnnotationSpanQuery.RangeJoin q) {
        final List<DUAAnnotationSpan> outerSpans = collectByTypeOnSofa(root, q.sofaFsRef(), q.outerTypeId());
        if (outerSpans.isEmpty()) return Stream.empty();
        final List<DUAAnnotationSpan> innerSpans = collectByTypeOnSofa(root, q.sofaFsRef(), q.innerTypeId());
        if (innerSpans.isEmpty()) return Stream.empty();

        return outerSpans.stream()
                .filter(outer -> innerSpans.stream().anyMatch(inner ->
                        outer.begin() <= inner.begin() && inner.end() <= outer.end()));
    }

    // ── CoveredText / Substring ───────────────────────────────────────────

    private Stream<DUAAnnotationSpan> queryCoveredText(
            final DUAAnnotationSpanQuery.CoveredText q) {
        return sofaStream(q.sofaFsRef())
                .filter(s -> s.coveredText().filter(q.text()::equals).isPresent())
                .filter(s -> q.typeId().isEmpty() || s.typeId() == q.typeId().getAsInt());
    }

    private Stream<DUAAnnotationSpan> querySubstring(
            final DUAAnnotationSpanQuery.Substring q) {
        return sofaStream(q.sofaFsRef())
                .filter(s -> s.coveredText().filter(text -> text.contains(q.text())).isPresent())
                .filter(s -> q.typeId().isEmpty() || s.typeId() == q.typeId().getAsInt());
    }

    /** Streams all spans on the given sofa from the tree (no sorting). */
    private Stream<DUAAnnotationSpan> sofaStream(final long sofaFsRef) {
        final List<DUAAnnotationSpan> result = new ArrayList<>();
        if (root != null) {
            collectAllOnSofa(root, sofaFsRef, result);
        }
        return result.stream();
    }

    // ── Pending-modification integration ──────────────────────────────────

    /**
     * Applies pending adds/removes on top of the tree results.
     * Removes are filtered out first, then pending adds are merged in.
     */
    private static Stream<DUAAnnotationSpan> applyPending(
            final Set<Long> removed,
            final List<DUAAnnotationSpan> adds,
            final Stream<DUAAnnotationSpan> treeResult) {
        return Stream.concat(
                treeResult.filter(s -> !removed.contains(s.fsRef())),
                adds.stream());
    }

    // ── Tree rebuild ──────────────────────────────────────────────────────

    /** Rebuilds the tree from scratch, incorporating all pending modifications. */
    private void rebuildTree() {
        final List<DUAAnnotationSpan> all = collectAllSpans();
        root = buildTree(all);
        pendingAdds.clear();
        pendingRemoves.clear();
    }

    /** Collects all spans (tree + pending adds, minus pending removes). */
    private List<DUAAnnotationSpan> collectAllSpans() {
        final List<DUAAnnotationSpan> all = new ArrayList<>();
        if (root != null) {
            collectSpans(root, all);
        }
        final Set<Long> removed = new HashSet<>(pendingRemoves);
        all.removeIf(s -> removed.contains(s.fsRef()));
        all.addAll(pendingAdds);
        return all;
    }

    private static void collectSpans(final Node node, final List<DUAAnnotationSpan> result) {
        if (node == null) return;
        result.addAll(node.byBegin);
        collectSpans(node.left, result);
        collectSpans(node.right, result);
    }

    // ── Tree construction ─────────────────────────────────────────────────

    /**
     * Builds an interval tree from the given list of spans.
     * <p>
     * Algorithm: sort by begin, pick the median as the centre value, partition
     * spans into left/centre/right, recursively build subtrees.
     */
    static Node buildTree(final List<DUAAnnotationSpan> spans) {
        if (spans == null || spans.isEmpty()) return null;
        if (spans.size() == 1) {
            final DUAAnnotationSpan s = spans.get(0);
            return new Node(s.begin(), List.of(s), List.of(s), null, null);
        }

        final List<DUAAnnotationSpan> sorted = new ArrayList<>(spans);
        sorted.sort(Comparator.comparingInt(DUAAnnotationSpan::begin)
                .thenComparingInt(DUAAnnotationSpan::end));

        final int mid = sorted.size() / 2;
        final int center = sorted.get(mid).begin();

        final List<DUAAnnotationSpan> leftSpans = new ArrayList<>();
        final List<DUAAnnotationSpan> centerSpans = new ArrayList<>();
        final List<DUAAnnotationSpan> rightSpans = new ArrayList<>();

        for (final DUAAnnotationSpan s : sorted) {
            if (s.end() <= center) {
                leftSpans.add(s);
            } else if (s.begin() > center) {
                rightSpans.add(s);
            } else {
                centerSpans.add(s);
            }
        }

        // byBegin: sorted by begin ASC, then end ASC
        final List<DUAAnnotationSpan> byBegin = new ArrayList<>(centerSpans);
        byBegin.sort(Comparator.comparingInt(DUAAnnotationSpan::begin)
                .thenComparingInt(DUAAnnotationSpan::end));

        // byEnd: sorted by end DESC, then begin ASC
        final List<DUAAnnotationSpan> byEnd = new ArrayList<>(centerSpans);
        byEnd.sort(Comparator.comparingInt(DUAAnnotationSpan::end).reversed()
                .thenComparingInt(DUAAnnotationSpan::begin));

        final Node left = leftSpans.isEmpty() ? null : buildTree(leftSpans);
        final Node right = rightSpans.isEmpty() ? null : buildTree(rightSpans);

        return new Node(center, byBegin, byEnd, left, right);
    }

    // ── Internal record for join key ──────────────────────────────────────

    private record SpanKey(int begin, int end) {}
}

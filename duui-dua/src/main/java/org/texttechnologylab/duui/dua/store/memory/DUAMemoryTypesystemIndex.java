package org.texttechnologylab.duui.dua.store.memory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUATypeQuery;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.store.DUATypesystemIndex;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

public final class DUAMemoryTypesystemIndex implements DUATypesystemIndex {

    /** Special type code for reference-edges returned as synthetic rows. */
    private static final int TYPE_REFERENCE = -1;

    private final CopyOnWriteArrayList<DUATypeNode> nodes = new CopyOnWriteArrayList<>();

    // ── Reference indexes ─────────────────────────────────────────────────

    /**
     * Reverse reference index: targetFsRef → featureCode → Set of sourceFsRef.
     * <p>
     * For a given target feature structure, tracks which source feature structures
     * reference it, grouped by the feature code of the reference.
     */
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Integer, CopyOnWriteArraySet<Long>>>
            reverseRefIndex = new ConcurrentHashMap<>();

    /**
     * Forward reference index: sourceFsRef → featureCode → Set of targetFsRef.
     * <p>
     * For a given source feature structure, tracks which target feature structures
     * it references, grouped by the feature code of the reference.
     */
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Integer, CopyOnWriteArraySet<Long>>>
            forwardRefIndex = new ConcurrentHashMap<>();

    // ── Write operations ──────────────────────────────────────────────────

    @Override
    public DUAWriteResult index(final DUATypeNode node) {
        nodes.add(Objects.requireNonNull(node, "node"));
        return new DUAWriteResult(DUAId.of("type-" + node.typeId()), new DUARevision(nodes.size()));
    }

    // ── Reference edge management ─────────────────────────────────────────

    /**
     * Records a reference edge from {@code sourceFsRef} to {@code targetFsRef}
     * via the feature identified by {@code featureCode}.
     * <p>
     * Updates both the forward and reverse index atomically from the perspective
     * of each index entry.
     *
     * @param sourceFsRef the source feature structure id
     * @param featureCode the code of the feature carrying the reference
     * @param targetFsRef the target feature structure id
     */
    public void addReferenceEdge(final long sourceFsRef,
                                 final int featureCode,
                                 final long targetFsRef) {
        // Forward: source → target
        forwardRefIndex
                .computeIfAbsent(sourceFsRef, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(featureCode, k -> new CopyOnWriteArraySet<>())
                .add(targetFsRef);

        // Reverse: target → source
        reverseRefIndex
                .computeIfAbsent(targetFsRef, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(featureCode, k -> new CopyOnWriteArraySet<>())
                .add(sourceFsRef);
    }

    /**
     * Removes a specific reference edge from {@code sourceFsRef} to
     * {@code targetFsRef} via the feature identified by {@code featureCode}.
     *
     * @param sourceFsRef the source feature structure id
     * @param featureCode the code of the feature carrying the reference
     * @param targetFsRef the target feature structure id
     */
    public void removeReferenceEdge(final long sourceFsRef,
                                    final int featureCode,
                                    final long targetFsRef) {
        // Forward: source → target
        final Map<Integer, CopyOnWriteArraySet<Long>> byFeatureFwd =
                forwardRefIndex.get(sourceFsRef);
        if (byFeatureFwd != null) {
            final CopyOnWriteArraySet<Long> targets = byFeatureFwd.get(featureCode);
            if (targets != null) {
                targets.remove(targetFsRef);
                if (targets.isEmpty()) {
                    byFeatureFwd.remove(featureCode);
                }
            }
            if (byFeatureFwd.isEmpty()) {
                forwardRefIndex.remove(sourceFsRef);
            }
        }

        // Reverse: target → source
        final Map<Integer, CopyOnWriteArraySet<Long>> byFeatureRev =
                reverseRefIndex.get(targetFsRef);
        if (byFeatureRev != null) {
            final CopyOnWriteArraySet<Long> sources = byFeatureRev.get(featureCode);
            if (sources != null) {
                sources.remove(sourceFsRef);
                if (sources.isEmpty()) {
                    byFeatureRev.remove(featureCode);
                }
            }
            if (byFeatureRev.isEmpty()) {
                reverseRefIndex.remove(targetFsRef);
            }
        }
    }

    /**
     * Removes <em>all</em> reference edges involving the given feature structure.
     * <p>
     * This must be called when a feature structure is deleted to keep the
     * reference indexes consistent.
     *
     * @param fsRef the feature structure id to remove
     */
    public void removeAllEdges(final long fsRef) {
        // Remove all forward edges originating from fsRef
        final Map<Integer, CopyOnWriteArraySet<Long>> byFeatureFwd =
                forwardRefIndex.remove(fsRef);
        if (byFeatureFwd != null) {
            // For each target, remove this source from reverse index
            for (final Map.Entry<Integer, CopyOnWriteArraySet<Long>> entry : byFeatureFwd.entrySet()) {
                final int featCode = entry.getKey();
                for (final Long targetRef : entry.getValue()) {
                    final Map<Integer, CopyOnWriteArraySet<Long>> revByFeature =
                            reverseRefIndex.get(targetRef);
                    if (revByFeature != null) {
                        final CopyOnWriteArraySet<Long> sources = revByFeature.get(featCode);
                        if (sources != null) {
                            sources.remove(fsRef);
                            if (sources.isEmpty()) {
                                revByFeature.remove(featCode);
                            }
                        }
                        if (revByFeature.isEmpty()) {
                            reverseRefIndex.remove(targetRef);
                        }
                    }
                }
            }
        }

        // Remove all reverse edges pointing to fsRef
        final Map<Integer, CopyOnWriteArraySet<Long>> byFeatureRev =
                reverseRefIndex.remove(fsRef);
        if (byFeatureRev != null) {
            // For each source, remove this target from forward index
            for (final Map.Entry<Integer, CopyOnWriteArraySet<Long>> entry : byFeatureRev.entrySet()) {
                final int featCode = entry.getKey();
                for (final Long sourceRef : entry.getValue()) {
                    final Map<Integer, CopyOnWriteArraySet<Long>> fwdByFeature =
                            forwardRefIndex.get(sourceRef);
                    if (fwdByFeature != null) {
                        final CopyOnWriteArraySet<Long> targets = fwdByFeature.get(featCode);
                        if (targets != null) {
                            targets.remove(fsRef);
                            if (targets.isEmpty()) {
                                fwdByFeature.remove(featCode);
                            }
                        }
                        if (fwdByFeature.isEmpty()) {
                            forwardRefIndex.remove(sourceRef);
                        }
                    }
                }
            }
        }
    }

    // ── Query ─────────────────────────────────────────────────────────────

    @Override
    public Stream<DUATypeNode> find(final DUATypeQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUATypeQuery.ExactType q -> nodes.stream()
                    .filter(node -> node.typeName().equals(q.typeName()));

            case DUATypeQuery.Subtypes q -> {
                final DUATypeNode root = byName(q.typeName());
                yield root == null
                        ? Stream.empty()
                        : nodes.stream()
                                .filter(node -> isSubtypeOf(node, root.typeId(), q.transitive()));
            }

            case DUATypeQuery.Supertypes q -> {
                final DUATypeNode leaf = byName(q.typeName());
                yield leaf == null
                        ? Stream.empty()
                        : supertypeNodes(leaf, q.transitive()).stream();
            }
            case DUATypeQuery.ReferenceTraversal q ->
                    // Reverse reference traversal: find sources that reference targetRef
                    reverseRefIndex
                            .getOrDefault(q.targetFsRef(), new ConcurrentHashMap<>())
                            .getOrDefault(q.featureCode(), new CopyOnWriteArraySet<>())
                            .stream()
                            .map(ref -> new DUATypeNode(
                                    (int) (long) ref,
                                    "__reference__",
                                    OptionalInt.of(TYPE_REFERENCE)));

            case DUATypeQuery.OutgoingReferences q ->
                    // Forward reference traversal: find targets referenced by sourceRef
                    forwardRefIndex
                            .getOrDefault(q.sourceFsRef(), new ConcurrentHashMap<>())
                            .getOrDefault(q.featureCode(), new CopyOnWriteArraySet<>())
                            .stream()
                            .map(ref -> new DUATypeNode(
                                    (int) (long) ref,
                                    "__reference__",
                                    OptionalInt.of(TYPE_REFERENCE)));
        };
    }

    // ── Hierarchy helpers ─────────────────────────────────────────────────

    private DUATypeNode byName(final String typeName) {
        return nodes.stream()
                .filter(node -> node.typeName().equals(typeName))
                .findFirst()
                .orElse(null);
    }

    private DUATypeNode byId(final int typeId) {
        return nodes.stream()
                .filter(node -> node.typeId() == typeId)
                .findFirst()
                .orElse(null);
    }

    private boolean isSubtypeOf(final DUATypeNode node,
                                final int parentTypeId,
                                final boolean transitive) {
        OptionalInt current = node.parentTypeId();
        if (current.isEmpty()) {
            return false;
        }
        if (current.getAsInt() == parentTypeId) {
            return true;
        }
        if (!transitive) {
            return false;
        }
        final DUATypeNode parent = byId(current.getAsInt());
        return parent != null && isSubtypeOf(parent, parentTypeId, true);
    }

    private List<DUATypeNode> supertypeNodes(final DUATypeNode leaf,
                                             final boolean transitive) {
        if (leaf.parentTypeId().isEmpty()) {
            return List.of();
        }
        final DUATypeNode parent = byId(leaf.parentTypeId().getAsInt());
        if (parent == null) {
            return List.of();
        }
        if (!transitive) {
            return List.of(parent);
        }
        return Stream.concat(
                Stream.of(parent),
                supertypeNodes(parent, true).stream()).toList();
    }
}

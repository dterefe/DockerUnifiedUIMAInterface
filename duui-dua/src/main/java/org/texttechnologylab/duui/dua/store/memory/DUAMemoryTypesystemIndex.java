package org.texttechnologylab.duui.dua.store.memory;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.DUAId;
import org.texttechnologylab.duui.dua.query.DUATypeQuery;
import org.texttechnologylab.duui.dua.store.DUARevision;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.store.DUATypesystemIndex;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;

public final class DUAMemoryTypesystemIndex implements DUATypesystemIndex {
    private final CopyOnWriteArrayList<DUATypeNode> nodes = new CopyOnWriteArrayList<>();

    @Override
    public DUAWriteResult index(DUATypeNode node) {
        nodes.add(Objects.requireNonNull(node, "node"));
        return new DUAWriteResult(DUAId.of("type-" + node.typeId()), new DUARevision(nodes.size()));
    }

    @Override
    public Stream<DUATypeNode> find(DUATypeQuery query) {
        Objects.requireNonNull(query, "query");
        return switch (query) {
            case DUATypeQuery.ExactType q -> nodes.stream()
                    .filter(node -> node.typeName().equals(q.typeName()));
            case DUATypeQuery.Subtypes q -> {
                DUATypeNode root = byName(q.typeName());
                yield root == null ? Stream.empty() : nodes.stream()
                        .filter(node -> isSubtypeOf(node, root.typeId(), q.transitive()));
            }
            case DUATypeQuery.Supertypes q -> {
                DUATypeNode leaf = byName(q.typeName());
                yield leaf == null ? Stream.empty() : supertypeNodes(leaf, q.transitive()).stream();
            }
            case DUATypeQuery.ReferenceTraversal q -> Stream.empty();
        };
    }

    private DUATypeNode byName(String typeName) {
        return nodes.stream()
                .filter(node -> node.typeName().equals(typeName))
                .findFirst()
                .orElse(null);
    }

    private DUATypeNode byId(int typeId) {
        return nodes.stream()
                .filter(node -> node.typeId() == typeId)
                .findFirst()
                .orElse(null);
    }

    private boolean isSubtypeOf(DUATypeNode node, int parentTypeId, boolean transitive) {
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
        DUATypeNode parent = byId(current.getAsInt());
        return parent != null && isSubtypeOf(parent, parentTypeId, true);
    }

    private List<DUATypeNode> supertypeNodes(DUATypeNode leaf, boolean transitive) {
        if (leaf.parentTypeId().isEmpty()) {
            return List.of();
        }
        DUATypeNode parent = byId(leaf.parentTypeId().getAsInt());
        if (parent == null) {
            return List.of();
        }
        if (!transitive) {
            return List.of(parent);
        }
        return Stream.concat(Stream.of(parent), supertypeNodes(parent, true).stream()).toList();
    }
}

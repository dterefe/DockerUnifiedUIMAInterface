package org.texttechnologylab.duui.dua.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class DUAGraphPartition {
    private final String id;
    private final String scope;
    private final List<DUAGraphNode> nodes = new ArrayList<>();
    private final List<DUAGraphEdge> edges = new ArrayList<>();

    public DUAGraphPartition(String id, String scope) {
        this.id = Objects.requireNonNull(id, "id");
        this.scope = scope == null ? "universe" : scope;
    }

    public String id() {
        return id;
    }

    public String scope() {
        return scope;
    }

    public DUAGraphPartition node(DUAGraphNode node) {
        nodes.add(Objects.requireNonNull(node, "node"));
        return this;
    }

    public DUAGraphPartition edge(DUAGraphEdge edge) {
        edges.add(Objects.requireNonNull(edge, "edge"));
        return this;
    }

    public Stream<DUAGraphNode> nodes() {
        return nodes.stream();
    }

    public Stream<DUAGraphEdge> edges() {
        return edges.stream();
    }

    public int nodeCount() {
        return nodes.size();
    }

    public int edgeCount() {
        return edges.size();
    }
}

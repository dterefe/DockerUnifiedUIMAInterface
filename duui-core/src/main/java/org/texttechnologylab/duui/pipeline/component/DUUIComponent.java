package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class DUUIComponent<T> implements DUUIActor, AutoCloseable {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final BlockingQueue<DUUINode<T>> nodes;
    private final int capacity;
    private final AutoCloseable closeAction;

    public DUUIComponent(String id, List<DUUINode<T>> nodes) {
        this(id, nodes, null);
    }

    public DUUIComponent(String id, List<DUUINode<T>> nodes, AutoCloseable closeAction) {
        String type = getClass().getSimpleName();
        this.gid = GID.create(type.isEmpty() ? DUUIComponent.class.getSimpleName() : type);
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        Objects.requireNonNull(nodes, "nodes");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("A DUUIComponent requires at least one node.");
        }
        this.nodes = new LinkedBlockingQueue<>(nodes);
        this.capacity = nodes.size();
        this.closeAction = closeAction;
    }

    @Override
    public GID gid() {
        return gid;
    }

    @Override
    public DUUITraits traits() {
        return traits;
    }

    @Override
    public String id() {
        return id;
    }

    public abstract DUUIFlow<DUUIArtifact<T>> process(DUUIArtifact<T> artifact);

    public int capacity() {
        return capacity;
    }

    public DUUINode<T> borrowNode() throws InterruptedException {
        return nodes.take();
    }

    public void returnNode(DUUINode<T> node) {
        nodes.offer(Objects.requireNonNull(node, "node"));
    }

    public int availableNodes() {
        return nodes.size();
    }

    public boolean fork() {
        return false;
    }

    @Override
    public void close() throws Exception {
        if (closeAction != null) {
            closeAction.close();
        }
    }
}

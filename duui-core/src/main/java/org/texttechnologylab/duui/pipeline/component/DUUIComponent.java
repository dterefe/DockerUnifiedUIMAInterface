package org.texttechnologylab.duui.pipeline.component;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIActor;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract component that processes artifacts.
 * Uses {@link DUUIPool} for node management (migrated from raw {@code LinkedBlockingQueue}).
 *
 * [DESIGN: lines 286, 288, 309]
 *
 * @param <T> artifact payload type
 */
public abstract class DUUIComponent<T> implements DUUIActor, AutoCloseable {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final DUUIPool<DUUINode<T>> pool;
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
        LinkedBlockingQueue<DUUINode<T>> nodeQueue = new LinkedBlockingQueue<>(nodes);
        this.pool = new DUUIPool<>(gid, id + "-nodes", nodeQueue);
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

    /**
     * Borrow a node from the pool (blocking).
     *
     * @return an available node
     * @throws InterruptedException if interrupted while waiting
     */
    public DUUINode<T> borrowNode() throws InterruptedException {
        return pool.take();
    }

    /**
     * Return a node to the pool.
     *
     * @param node the node to return
     */
    public void returnNode(DUUINode<T> node) {
        pool.offer(Objects.requireNonNull(node, "node"));
    }

    public int availableNodes() {
        return pool.depth();
    }
/**
 * Iterate all nodes currently in the pool.
 *
 * @return list of all DUUINodes
 */
public List<DUUINode<T>> nodes() {
    java.util.ArrayList<DUUINode<T>> all = new java.util.ArrayList<>();
    for (int i = 0; i < capacity; i++) {
        try {
            DUUINode<T> n = pool.take();
            all.add(n);
            pool.offer(n);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
        }
    }
    return all;
}

    public DUUIPool<DUUINode<T>> nodePool() {
        return pool;
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

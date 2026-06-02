package org.texttechnologylab.duui.clients.hosts.virtualization;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.time.Instant;

public abstract class DUUIContainer implements DUUIProxy {
    private final DUUIAddress address;
    private final String id;
    private final DUUIContainerImage image;
    private final Instant createdAt;

    protected DUUIContainer(DUUIAddress address, String id, DUUIContainerImage image, Instant createdAt) {
        this.address = address;
        this.id = id;
        this.image = image;
        this.createdAt = createdAt;
    }

    @Override
    public final DUUIAddress address() {
        return address;
    }

    public final String id() {
        return id;
    }

    public final DUUIContainerImage image() {
        return image;
    }

    public final Instant createdAt() {
        return createdAt;
    }

    public abstract DUUIFlow<Boolean> running() throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainer> start() throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainer> stop() throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainer> restart() throws DUUIVirtualizationException;

    public abstract DUUIFlow<Void> delete() throws DUUIVirtualizationException;
}

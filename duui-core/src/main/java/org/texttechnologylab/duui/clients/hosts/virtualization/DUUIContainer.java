package org.texttechnologylab.duui.clients.hosts.virtualization;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;

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

    public abstract boolean running() throws DUUIVirtualizationException;

    public abstract DUUIContainer start() throws DUUIVirtualizationException;

    public abstract DUUIContainer stop() throws DUUIVirtualizationException;

    public abstract DUUIContainer restart() throws DUUIVirtualizationException;

    public abstract void delete() throws DUUIVirtualizationException;
}

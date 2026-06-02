package org.texttechnologylab.duui.clients.hosts.virtualization;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;
import org.texttechnologylab.duui.clients.handle.DUUIProxy;
import org.texttechnologylab.duui.timelines.DUUIFlow;

import java.time.Instant;
import java.util.List;

public abstract class DUUIContainerImage implements DUUIProxy {
    private final DUUIAddress address;
    private final String reference;
    private final long size;
    private final Instant createdAt;

    protected DUUIContainerImage(DUUIAddress address, String reference, long size, Instant createdAt) {
        this.address = address;
        this.reference = reference;
        this.size = size;
        this.createdAt = createdAt;
    }

    @Override
    public final DUUIAddress address() {
        return address;
    }

    public final String reference() {
        return reference;
    }

    public final long size() {
        return size;
    }

    public final Instant createdAt() {
        return createdAt;
    }

    public abstract DUUIFlow<? extends DUUIContainer> run(List<String> command) throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainerImage> pull() throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainerImage> push() throws DUUIVirtualizationException;

    public abstract DUUIFlow<DUUIContainerImage> build(String context) throws DUUIVirtualizationException;
}

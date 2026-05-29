package org.texttechnologylab.duui.clients.hosts.virtualization;

import org.texttechnologylab.duui.clients.handle.DUUIAddress;

import java.util.stream.Stream;

public abstract class DUUIVirtualizationClient<C extends DUUIContainer, I extends DUUIContainerImage> {
    private final DUUIAddress address;

    protected DUUIVirtualizationClient(DUUIAddress address) {
        this.address = address;
    }

    public final DUUIAddress address() {
        return address;
    }

    public abstract I image(String reference);

    public abstract C container(String id) throws DUUIVirtualizationException;

    public abstract Stream<C> containers() throws DUUIVirtualizationException;
}

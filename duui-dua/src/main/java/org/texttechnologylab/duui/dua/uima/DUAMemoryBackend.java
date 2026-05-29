package org.texttechnologylab.duui.dua.uima;

import org.apache.uima.cas.impl.Backend;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUACasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;

/**
 * Compatibility name for the default in-process DUA backend.
 */
public final class DUAMemoryBackend implements Backend {
    private final DUAStorageBackend delegate;

    public DUAMemoryBackend() {
        this(new DUAConcurrentMemoryCasStorage());
    }

    public DUAMemoryBackend(DUACasStorage storage) {
        this.delegate = new DUAStorageBackend(storage);
    }

    public DUACasStorage storage() {
        return delegate.storage();
    }

    @Override
    public SlotBackend slots() {
        return delegate.slots();
    }

    @Override
    public ArrayBackend arrays() {
        return delegate.arrays();
    }

    @Override
    public CollectionBackend collections() {
        return delegate.collections();
    }

    @Override
    public StringBackend strings() {
        return delegate.strings();
    }

    @Override
    public LifecycleBackend lifecycle() {
        return delegate.lifecycle();
    }
}

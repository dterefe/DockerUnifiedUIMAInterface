package org.texttechnologylab.duui.dua.store.memory;

import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUATextQuery;
import org.texttechnologylab.duui.dua.store.DUATextQueryStore;
import org.texttechnologylab.duui.dua.store.DUATextRow;

public final class DUAMemoryTextQueryStore implements DUATextQueryStore {
    @Override
    public Stream<DUATextRow> find(DUATextQuery query) {
        return Stream.empty();
    }
}

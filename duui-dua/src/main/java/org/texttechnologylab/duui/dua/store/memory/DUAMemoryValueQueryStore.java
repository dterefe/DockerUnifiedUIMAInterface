package org.texttechnologylab.duui.dua.store.memory;

import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUAValueQuery;
import org.texttechnologylab.duui.dua.store.DUAValueQueryStore;
import org.texttechnologylab.duui.dua.store.DUAValueRow;

public final class DUAMemoryValueQueryStore implements DUAValueQueryStore {
    @Override
    public Stream<DUAValueRow> find(DUAValueQuery query) {
        return Stream.empty();
    }
}

package org.texttechnologylab.duui.dua.store;

import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUAValueQuery;

public interface DUAValueQueryStore {
    Stream<DUAValueRow> find(DUAValueQuery query);
}

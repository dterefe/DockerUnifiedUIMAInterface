package org.texttechnologylab.duui.dua.store;

import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUATextQuery;

public interface DUATextQueryStore {
    Stream<DUATextRow> find(DUATextQuery query);
}

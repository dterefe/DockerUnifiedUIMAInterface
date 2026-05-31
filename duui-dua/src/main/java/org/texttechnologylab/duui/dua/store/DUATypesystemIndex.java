package org.texttechnologylab.duui.dua.store;

import java.util.stream.Stream;

import org.texttechnologylab.duui.dua.query.DUATypeQuery;

public interface DUATypesystemIndex {
    DUAWriteResult index(DUATypeNode node);

    Stream<DUATypeNode> find(DUATypeQuery query);
}

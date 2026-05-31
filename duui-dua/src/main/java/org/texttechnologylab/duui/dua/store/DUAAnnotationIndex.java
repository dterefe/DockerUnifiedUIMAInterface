package org.texttechnologylab.duui.dua.store;

import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;

import java.util.stream.Stream;

public interface DUAAnnotationIndex {
    DUAWriteResult index(DUAAnnotationSpan span);

    Stream<DUAAnnotationSpan> find(DUAAnnotationSpanQuery query);
}

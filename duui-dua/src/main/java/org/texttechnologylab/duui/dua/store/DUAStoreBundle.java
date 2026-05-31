package org.texttechnologylab.duui.dua.store;

import java.util.Objects;

import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.store.memory.DUAMemoryAnnotationIndex;
import org.texttechnologylab.duui.dua.store.memory.DUAMemoryTextQueryStore;
import org.texttechnologylab.duui.dua.store.memory.DUAMemoryTypesystemIndex;
import org.texttechnologylab.duui.dua.store.memory.DUAMemoryValueQueryStore;
import org.texttechnologylab.duui.dua.uima.storage.DUACasStorage;

public record DUAStoreBundle(
        DUACasStorage casStorage,
        DUAAnnotationIndex annotationIndex,
        DUATypesystemIndex typesystemIndex,
        DUAValueQueryStore values,
        DUATextQueryStore texts,
        DUABackendLayout layout
) {
    public DUAStoreBundle {
        Objects.requireNonNull(casStorage, "casStorage");
        Objects.requireNonNull(annotationIndex, "annotationIndex");
        Objects.requireNonNull(typesystemIndex, "typesystemIndex");
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(texts, "texts");
        layout = layout == null ? DUABackendLayout.inMemory() : layout;
    }

    public static DUAStoreBundle inMemory(DUACasStorage casStorage) {
        return new DUAStoreBundle(
                casStorage,
                new DUAMemoryAnnotationIndex(),
                new DUAMemoryTypesystemIndex(),
                new DUAMemoryValueQueryStore(),
                new DUAMemoryTextQueryStore(),
                DUABackendLayout.inMemory());
    }

    public static DUAStoreBundle of(DUACasStorage casStorage,
                                    DUAAnnotationIndex annotationIndex,
                                    DUABackendLayout layout) {
        return of(casStorage, annotationIndex, new DUAMemoryTypesystemIndex(), layout);
    }

    public static DUAStoreBundle of(DUACasStorage casStorage,
                                    DUAAnnotationIndex annotationIndex,
                                    DUATypesystemIndex typesystemIndex,
                                    DUABackendLayout layout) {
        return new DUAStoreBundle(
                casStorage,
                annotationIndex,
                typesystemIndex,
                new DUAMemoryValueQueryStore(),
                new DUAMemoryTextQueryStore(),
                layout);
    }
}

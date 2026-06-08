package org.texttechnologylab.duui.dua.backend.postgres;

import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.store.DUAStoreBundle;
import org.texttechnologylab.duui.dua.uima.storage.DUACasStorage;

import java.util.Objects;

public final class DUAPostgresStoreBundle {
    private DUAPostgresStoreBundle() {
    }

    public static DUAStoreBundle over(DUACasStorage casStorage, DUAPostgresConnectionProvider connections) {
        return over(casStorage, connections, DUAPostgresRangeType.INT4);
    }

    public static DUAStoreBundle over(DUACasStorage casStorage,
                                      DUAPostgresConnectionProvider connections,
                                      DUAPostgresRangeType rangeType) {
        Objects.requireNonNull(connections, "connections");
        return new DUAStoreBundle(
                casStorage,
                new DUAPostgresAnnotationIndex(connections, DUAPostgresAnnotationIndex.DEFAULT_TABLE, rangeType),
                new DUAPostgresTypesystemIndex(connections),
                new DUAPostgresValueQueryStore(connections),
                new DUAPostgresTextQueryStore(connections),
                DUABackendLayout.postgres());
    }
}

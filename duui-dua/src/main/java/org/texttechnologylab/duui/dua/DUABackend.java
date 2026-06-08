package org.texttechnologylab.duui.dua;

import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.Backend;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.duui.dua.backend.DUABackendLayout;
import org.texttechnologylab.duui.dua.backend.postgres.DUAPostgresConnectionProvider;
import org.texttechnologylab.duui.dua.backend.postgres.DUAPostgresRangeType;
import org.texttechnologylab.duui.dua.backend.postgres.DUAPostgresStoreBundle;
import org.texttechnologylab.duui.dua.projection.DUAProjection;
import org.texttechnologylab.duui.dua.projection.DUAProjectionType;
import org.texttechnologylab.duui.dua.projection.DUAProjectionTypes;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpan;
import org.texttechnologylab.duui.dua.query.DUAAnnotationSpanQuery;
import org.texttechnologylab.duui.dua.store.DUAAnnotationIndex;
import org.texttechnologylab.duui.dua.store.DUAProjectionStore;
import org.texttechnologylab.duui.dua.store.DUAStoreBundle;
import org.texttechnologylab.duui.dua.store.DUATextQueryStore;
import org.texttechnologylab.duui.dua.store.DUATypesystemIndex;
import org.texttechnologylab.duui.dua.store.DUATypeNode;
import org.texttechnologylab.duui.dua.store.DUAValueQueryStore;
import org.texttechnologylab.duui.dua.store.DUAWriteResult;
import org.texttechnologylab.duui.dua.uima.DUACasBackendInstaller;
import org.texttechnologylab.duui.dua.uima.storage.DUACasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUACasValue;
import org.texttechnologylab.duui.dua.uima.storage.DUAConcurrentMemoryCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUASqliteCasStorage;
import org.texttechnologylab.duui.dua.uima.storage.DUAStorageBackend;

public final class DUABackend implements AutoCloseable {
    private final DUAStorageBackend casBackendAdapter;
    private final DUAStoreBundle stores;
    private final DUAProjectionStore projections;

    private DUABackend(DUAStoreBundle stores) {
        this.stores = Objects.requireNonNull(stores, "stores");
        this.casBackendAdapter = new DUAStorageBackend(stores.casStorage(), stores);
        this.projections = new DUAProjectionStore();
        registerBuiltInProjections();
    }

    public static DUABackend inMemory() { return over(new DUAConcurrentMemoryCasStorage()); }
    public static DUABackend sqlite(Path path) { return over(new DUASqliteCasStorage(path)); }
    public static DUABackend over(DUACasStorage casStorage) { return new DUABackend(DUAStoreBundle.inMemory(casStorage)); }
    public static DUABackend over(DUACasStorage cs, DUAAnnotationIndex ai) {
        return new DUABackend(DUAStoreBundle.of(cs, ai, DUABackendLayout.inMemory()));
    }
    public static DUABackend over(DUACasStorage cs, DUAAnnotationIndex ai, DUABackendLayout l) {
        return new DUABackend(DUAStoreBundle.of(cs, ai, l));
    }
    public static DUABackend over(DUAStoreBundle stores) { return new DUABackend(stores); }
    public static DUABackend postgres(DUACasStorage cs, DUAPostgresConnectionProvider c) {
        return over(DUAPostgresStoreBundle.over(cs, c));
    }
    public static DUABackend postgres(DUACasStorage cs, DUAPostgresConnectionProvider c, DUAPostgresRangeType r) {
        return over(DUAPostgresStoreBundle.over(cs, c, r));
    }

    public DUACasStorage casStorage() { return stores.casStorage(); }
    public Backend apacheBackendAdapter() { return casBackendAdapter; }
    public DUAProjectionStore projections() { return projections; }
    public DUABackendLayout layout() { return stores.layout(); }
    public DUAStoreBundle stores() { return stores; }
    public DUAAnnotationIndex annotationIndex() { return stores.annotationIndex(); }
    public DUATypesystemIndex typesystemIndex() { return stores.typesystemIndex(); }
    public DUAValueQueryStore values() { return stores.values(); }
    public DUATextQueryStore texts() { return stores.texts(); }

    public <T> DUAProjection<T> registerProjection(DUAProjectionType<T> type) { return projections.register(type); }
    public <T> JDUA<T> project(JCas view, DUAProjectionType<T> type) {
        Objects.requireNonNull(view, "view");
        DUAProjection<T> projection = registerProjection(type);
        installInto(view);
        return new JDUA<>(view, this, projection);
    }
    public void installInto(JCas view) { DUACasBackendInstaller.install(view, apacheBackendAdapter()); }
    public void installInto(CAS cas) { DUACasBackendInstaller.install(cas, apacheBackendAdapter()); }
    private void registerBuiltInProjections() {
        registerProjection(DUAProjectionTypes.CORPUS);
        registerProjection(DUAProjectionTypes.DOCUMENT);
    }

    public OptionalInt readIntSlot(int fsRef, int featureCode, String featureName) {
        return stores.casStorage().readSlot(fsRef, featureCode, featureName)
                .map(DUACasValue::intValue).map(OptionalInt::of).orElseGet(OptionalInt::empty);
    }
    public void writeIntSlot(int fsRef, int featureCode, String featureName, int value) {
        stores.casStorage().writeSlot(fsRef, featureCode, featureName, DUACasValue.ofInt(value));
    }
    public DUAWriteResult indexAnnotation(DUAAnnotationSpan span) { return stores.annotationIndex().index(span); }
    public DUAWriteResult indexType(DUATypeNode node) { return stores.typesystemIndex().index(node); }
    public Stream<DUAAnnotationSpan> findAnnotations(DUAAnnotationSpanQuery query) { return stores.annotationIndex().find(query); }
    @Override
    public void close() { stores.casStorage().close(); }
}

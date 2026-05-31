package org.texttechnologylab.duui.dua.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.ANNOTATION_SPAN_LOOKUP;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.ARCHIVE_BYTES;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.CAS_SLOT_VALUES;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.COVERED_TEXT_LOOKUP;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.FEATURE_REFERENCE_TRAVERSAL;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.FS_COLLECTION_VALUES;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.FS_ID_ALLOCATION;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.RANGE_JOIN;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.SUBSTRING_LOOKUP;
import static org.texttechnologylab.duui.dua.backend.DUAStoreCapability.TYPE_HIERARCHY;
import static org.texttechnologylab.duui.dua.backend.DUAStoreRole.ANNOTATION_RANGE;
import static org.texttechnologylab.duui.dua.backend.DUAStoreRole.ARCHIVE_PAYLOAD;
import static org.texttechnologylab.duui.dua.backend.DUAStoreRole.RELATIONAL_VALUE;
import static org.texttechnologylab.duui.dua.backend.DUAStoreRole.TEXT_SEARCH;
import static org.texttechnologylab.duui.dua.backend.DUAStoreRole.TYPESYSTEM_GRAPH;

public final class DUABackendLayout {
    private String id;
    private String description;
    private List<DUABackendStore> stores = new ArrayList<>();

    public DUABackendLayout() {
    }

    public DUABackendLayout(String id, String description, List<DUABackendStore> stores) {
        this.id = Objects.requireNonNull(id, "id");
        this.description = Objects.requireNonNull(description, "description");
        this.stores = new ArrayList<>(Objects.requireNonNull(stores, "stores"));
    }

    public static DUABackendLayout inMemory() {
        return new DUABackendLayout("duui-dua-memory", "In-process DUA backend layout", List.of(
                DUABackendStore.builder("cas-values", RELATIONAL_VALUE, "duui-cas-storage")
                        .capability(CAS_SLOT_VALUES)
                        .capability(FS_COLLECTION_VALUES)
                        .capability(FS_ID_ALLOCATION)
                        .parameter("mapsTo", "DUACasStorage")
                        .build(),
                DUABackendStore.builder("annotation-spans", ANNOTATION_RANGE, "memory-range-index")
                        .capability(ANNOTATION_SPAN_LOOKUP)
                        .capability(RANGE_JOIN)
                        .capability(COVERED_TEXT_LOOKUP)
                        .parameter("api", "DUAAnnotationSpanQuery")
                        .build(),
                DUABackendStore.builder("type-system", TYPESYSTEM_GRAPH, "uima-type-system-adapter")
                        .capability(TYPE_HIERARCHY)
                        .capability(FEATURE_REFERENCE_TRAVERSAL)
                        .parameter("api", "DUATypeQuery")
                        .build(),
                DUABackendStore.builder("sofa-text", TEXT_SEARCH, "memory-text-index")
                        .capability(SUBSTRING_LOOKUP)
                        .capability(COVERED_TEXT_LOOKUP)
                        .parameter("api", "DUATextQuery")
                        .build(),
                DUABackendStore.builder("archive-payloads", ARCHIVE_PAYLOAD, "zip-payload-store")
                        .capability(ARCHIVE_BYTES)
                        .build()));
    }

    public static DUABackendLayout postgres() {
        return new DUABackendLayout("duui-dua-postgres", "PostgreSQL-backed DUA semantic store layout", List.of(
                DUABackendStore.builder("pg-cas-values", RELATIONAL_VALUE, "postgresql-relational-values")
                        .capability(CAS_SLOT_VALUES)
                        .capability(FS_COLLECTION_VALUES)
                        .capability(FS_ID_ALLOCATION)
                        .parameter("tables", "dua_feature_structures,dua_feature_values,dua_array_values,dua_fs_counter")
                        .build(),
                DUABackendStore.builder("pg-annotation-ranges", ANNOTATION_RANGE, "postgresql-range-gist")
                        .capability(ANNOTATION_SPAN_LOOKUP)
                        .capability(RANGE_JOIN)
                        .capability(COVERED_TEXT_LOOKUP)
                        .parameter("extensions", "btree_gist")
                        .parameter("rangeTypes", "int4range,int8range")
                        .build(),
                DUABackendStore.builder("pg-type-graph", TYPESYSTEM_GRAPH, "postgresql-type-graph")
                        .capability(TYPE_HIERARCHY)
                        .capability(FEATURE_REFERENCE_TRAVERSAL)
                        .parameter("extensions", "age")
                        .parameter("tables", "dua_types,dua_type_features,dua_feature_references")
                        .build(),
                DUABackendStore.builder("pg-text", TEXT_SEARCH, "postgresql-text-search")
                        .capability(SUBSTRING_LOOKUP)
                        .capability(COVERED_TEXT_LOOKUP)
                        .parameter("extensions", "pg_trgm")
                        .parameter("tables", "dua_sofas,dua_text_segments")
                        .build(),
                DUABackendStore.builder("archive-payloads", ARCHIVE_PAYLOAD, "dua-archive-payload-store")
                        .capability(ARCHIVE_BYTES)
                        .build()));
    }

    public Optional<DUABackendStore> store(DUAStoreRole role) {
        return stores.stream().filter(store -> store.role() == role).findFirst();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<DUABackendStore> getStores() {
        return stores;
    }

    public void setStores(List<DUABackendStore> stores) {
        this.stores = stores == null ? new ArrayList<>() : new ArrayList<>(stores);
    }
}

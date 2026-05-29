package org.texttechnologylab.duui.dua.service;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class DUAServiceModuleCatalog {
    public static final String CORE_CAS_SERVICE_ID = "dua-core.cas-shard";
    public static final String CORE_REGISTRY_SERVICE_ID = "dua-core.corpus-registry";
    public static final String CORE_PIPELINE_WINDOW_SERVICE_ID = "dua-core.pipeline-window";
    public static final String EVENT_LOG_SERVICE_ID = "dua-events.revision-stream";

    private final List<DUAServiceModule> modules;
    private final Map<String, DUAServiceContract> servicesById;

    private DUAServiceModuleCatalog(List<DUAServiceModule> modules) {
        this.modules = List.copyOf(modules);
        this.servicesById = indexServices(modules);
    }

    public static DUAServiceModuleCatalog coreCatalog() {
        List<DUAServiceModule> modules = new ArrayList<>();
        modules.add(coreModule());
        modules.add(transportModule());
        modules.add(pipelineRuntimeModule());
        modules.add(eventModule());
        modules.add(queryCoordinationModule());
        modules.add(fulltextModule());
        modules.add(annotationAnalyticsModule());
        modules.add(metadataOntologyModule());
        modules.add(semanticEventModule());
        modules.add(geoTemporalModule());
        modules.add(vectorModule());
        modules.add(graphNavigationModule());
        modules.add(inspectorModule());
        modules.add(governanceModule());
        modules.add(observabilityModule());
        return new DUAServiceModuleCatalog(modules);
    }

    public List<DUAServiceModule> modules() {
        return modules;
    }

    public List<DUAServiceContract> services() {
        return servicesById.values().stream().toList();
    }

    public Optional<DUAServiceContract> findService(String id) {
        return Optional.ofNullable(servicesById.get(id));
    }

    public List<DUAServiceContract> servicesByKind(DUAServiceModuleKind kind) {
        return services().stream().filter(service -> service.moduleKind() == kind).toList();
    }

    public List<DUAServiceContract> canonicalAuthorities() {
        return services().stream().filter(DUAServiceContract::canonicalAuthority).toList();
    }

    private static Map<String, DUAServiceContract> indexServices(List<DUAServiceModule> modules) {
        Map<String, DUAServiceContract> byId = new LinkedHashMap<>();
        for (DUAServiceModule module : modules) {
            for (DUAServiceContract service : module.services()) {
                DUAServiceContract previous = byId.put(service.id(), service);
                if (previous != null) {
                    throw new IllegalArgumentException("duplicate service id: " + service.id());
                }
            }
        }
        return Map.copyOf(byId);
    }

    private static DUAServiceModule coreModule() {
        return module("dua-core", "DUA Core Store", DUAServiceModuleKind.CORE,
                "Canonical corpus, document, payload, type system, revision, and CAS keyspace authority.",
                service(CORE_REGISTRY_SERVICE_ID, "dua-core", "CorpusRegistryService", DUAServiceModuleKind.CORE,
                        DUAServicePerformanceClass.HOT_PATH,
                        protocols(DUAServiceProtocol.IN_PROCESS, DUAServiceProtocol.GRPC),
                        data(DUAServiceDataClass.CANONICAL_CORPUS_REGISTRY, DUAServiceDataClass.TYPE_SYSTEM),
                        data(),
                        List.of(),
                        true,
                        false,
                        "Universe, corpus, document, view, membership, type system, and shard routing lookup.",
                        "O(1) identity lookup on hot membership and type metadata; must scale before every accelerator."),
                service(CORE_CAS_SERVICE_ID, "dua-core", "CasShardService", DUAServiceModuleKind.CORE,
                        DUAServicePerformanceClass.HOT_PATH,
                        protocols(DUAServiceProtocol.IN_PROCESS, DUAServiceProtocol.GRPC, DUAServiceProtocol.NATIVE_DRIVER),
                        data(DUAServiceDataClass.CANONICAL_CAS, DUAServiceDataClass.CANONICAL_PAYLOAD,
                                DUAServiceDataClass.REVISION_LOG),
                        data(),
                        List.of(required(CORE_REGISTRY_SERVICE_ID, "Resolve corpus/document/view/type shard identity.",
                                DUAServiceProtocol.IN_PROCESS)),
                        true,
                        false,
                        "JCas-compatible slot, array, sofa, string, lifecycle, and payload operations.",
                        "Fastest path; direct key/range reads, lazy materialization, virtual-thread friendly point IO."),
                service(CORE_PIPELINE_WINDOW_SERVICE_ID, "dua-core", "PipelineWindowService", DUAServiceModuleKind.CORE,
                        DUAServicePerformanceClass.LOW_LATENCY,
                        protocols(DUAServiceProtocol.IN_PROCESS, DUAServiceProtocol.GRPC),
                        data(DUAServiceDataClass.PIPELINE_RUN_STATE),
                        data(),
                        List.of(required(CORE_CAS_SERVICE_ID, "Lease document windows and commit result revisions.",
                                DUAServiceProtocol.IN_PROCESS)),
                        true,
                        false,
                        "Corpus/document window leasing, idempotent result commits, and concurrent pipeline safety.",
                        "Low-latency scheduler state; avoids loading whole corpora into Java heap."));
    }

    private static DUAServiceModule transportModule() {
        return module("dua-transport", "DUA Transport", DUAServiceModuleKind.TRANSPORT,
                "Import/export boundary for XMI and DUA document-transfer packages.",
                service("dua-transport.document-transfer", "dua-transport", "DocumentTransferService",
                        DUAServiceModuleKind.TRANSPORT, DUAServicePerformanceClass.HIGH_THROUGHPUT,
                        protocols(DUAServiceProtocol.IN_PROCESS, DUAServiceProtocol.GRPC, DUAServiceProtocol.HTTP_OPENAPI,
                                DUAServiceProtocol.OBJECT_STORE),
                        data(DUAServiceDataClass.TRANSPORT_PACKAGE),
                        data(),
                        List.of(required(CORE_REGISTRY_SERVICE_ID, "Resolve target corpus/document membership.",
                                        DUAServiceProtocol.GRPC),
                                required(CORE_CAS_SERVICE_ID, "Materialize/import XMI or native document shards.",
                                        DUAServiceProtocol.GRPC)),
                        false,
                        false,
                        "Single/multi-document movement, XMI compatibility, FS id remap, and corpus membership patches.",
                        "Batch throughput oriented; correctness depends on explicit identity mode and checksum validation."));
    }

    private static DUAServiceModule pipelineRuntimeModule() {
        return module("dua-pipeline-runtime", "DUA Pipeline Runtime", DUAServiceModuleKind.PIPELINE_RUNTIME,
                "DUUI-compatible orchestration adapters and worker scheduling around DUA corpus windows.",
                service("dua-pipeline-runtime.worker-scheduler", "dua-pipeline-runtime", "WorkerSchedulerService",
                        DUAServiceModuleKind.PIPELINE_RUNTIME, DUAServicePerformanceClass.HIGH_THROUGHPUT,
                        protocols(DUAServiceProtocol.GRPC, DUAServiceProtocol.EVENT_STREAM, DUAServiceProtocol.HTTP_OPENAPI),
                        data(DUAServiceDataClass.PIPELINE_RUN_STATE),
                        data(DUAServiceDataClass.TELEMETRY),
                        List.of(required(CORE_PIPELINE_WINDOW_SERVICE_ID,
                                        "Acquire work windows and publish commit decisions.", DUAServiceProtocol.GRPC),
                                optional(EVENT_LOG_SERVICE_ID, "Publish run and stage events for projections.",
                                        DUAServiceProtocol.EVENT_STREAM)),
                        false,
                        false,
                        "High-concurrency corpus processing with DUUI annotators, lazy JCas materialization, and retries.",
                        "Throughput bound by annotators and CAS materialization, not by corpus-size heap growth."));
    }

    private static DUAServiceModule eventModule() {
        return module("dua-events", "DUA Projection Event Log", DUAServiceModuleKind.EVENT_LOG,
                "Append-only change stream for rebuilding and updating derived modules.",
                service(EVENT_LOG_SERVICE_ID, "dua-events", "RevisionStreamService",
                        DUAServiceModuleKind.EVENT_LOG, DUAServicePerformanceClass.HIGH_THROUGHPUT,
                        protocols(DUAServiceProtocol.EVENT_STREAM, DUAServiceProtocol.GRPC),
                        data(DUAServiceDataClass.PROJECTION_EVENT),
                        data(),
                        List.of(required(CORE_CAS_SERVICE_ID, "Consume committed CAS revisions and payload pointers.",
                                DUAServiceProtocol.GRPC)),
                        false,
                        false,
                        "Fan-out to query projections, inspector projections, telemetry, and rebuild jobs.",
                        "Sequential append and replay; preserves core latency by decoupling expensive indexes."));
    }

    private static DUAServiceModule queryCoordinationModule() {
        return module("dua-query", "DUA Query Coordination", DUAServiceModuleKind.QUERY_COORDINATION,
                "Federates accelerator candidate sets and returns stable DUA object references.",
                service("dua-query.coordinator", "dua-query", "QueryCoordinatorService",
                        DUAServiceModuleKind.QUERY_COORDINATION, DUAServicePerformanceClass.LOW_LATENCY,
                        protocols(DUAServiceProtocol.GRPC, DUAServiceProtocol.HTTP_OPENAPI, DUAServiceProtocol.ARROW_FLIGHT),
                        data(),
                        data(DUAServiceDataClass.DERIVED_INDEX),
                        List.of(required(CORE_REGISTRY_SERVICE_ID, "Resolve query scopes and readable corpora.",
                                        DUAServiceProtocol.GRPC),
                                required(CORE_CAS_SERVICE_ID, "Materialize final snippets or FS refs after candidate pruning.",
                                        DUAServiceProtocol.GRPC),
                                optional("dua-fulltext.text-index", "Intersect lexical candidate sets.",
                                        DUAServiceProtocol.GRPC),
                                optional("dua-annotation-analytics.annotation-facts", "Intersect annotation fact sets.",
                                        DUAServiceProtocol.ARROW_FLIGHT),
                                optional("dua-metadata-ontology.metadata-index", "Intersect metadata and ontology sets.",
                                        DUAServiceProtocol.JDBC),
                                optional("dua-semantic-events.event-index", "Intersect event/frame candidate sets.",
                                        DUAServiceProtocol.ARROW_FLIGHT),
                                optional("dua-geo-temporal.spacetime-index", "Intersect spatial/temporal candidates.",
                                        DUAServiceProtocol.JDBC),
                                optional("dua-vector.vector-index", "Fetch nearest-neighbor candidates.",
                                        DUAServiceProtocol.GRPC)),
                        false,
                        false,
                        "Complex corpus-wide queries without forcing the core CAS backend to become a search database.",
                        "Fast when accelerators return compact ID sets; can fall back to slow core scans for correctness."));
    }

    private static DUAServiceModule fulltextModule() {
        return accelerator("dua-fulltext", "DUA Fulltext Accelerator",
                "Lexical search, phrase search, snippets, highlighting, language analyzers, and OCR text search.",
                "dua-fulltext.text-index", "TextIndexService",
                data(DUAServiceDataClass.DERIVED_INDEX),
                "Fulltext is intentionally outside the CAS hot path; it returns corpus/document/view/FS candidate IDs.",
                "Sublinear lexical lookup when indexed; weak fallback is full CAS text scan through core.");
    }

    private static DUAServiceModule annotationAnalyticsModule() {
        return accelerator("dua-annotation-analytics", "DUA Annotation Analytics Accelerator",
                "Columnar annotation facts for timelines, facets, histograms, co-occurrence, and inspector aggregations.",
                "dua-annotation-analytics.annotation-facts", "AnnotationFactService",
                data(DUAServiceDataClass.DERIVED_ANALYTIC),
                "Wide scans over begin/end/type/value/reference facts, not full FS materialization.",
                "Columnar/vectorized scans over projected facts; much faster than per-document JCas iteration.");
    }

    private static DUAServiceModule metadataOntologyModule() {
        return accelerator("dua-metadata-ontology", "DUA Metadata And Ontology Accelerator",
                "Metadata predicates, controlled vocabularies, external IDs, taxonomy, GBIF/UCE-style algebra adapters.",
                "dua-metadata-ontology.metadata-index", "MetadataOntologyService",
                data(DUAServiceDataClass.DERIVED_INDEX),
                "Structured metadata and ontology joins over DUA IDs while core remains identifier-first.",
                "Fast indexed joins for broad corpus filtering; avoids graph explosion for simple references.");
    }

    private static DUAServiceModule semanticEventModule() {
        return accelerator("dua-semantic-events", "DUA Semantic Event Accelerator",
                "Event/frame/SRL projections for cross-document event timelines and role-aware search.",
                "dua-semantic-events.event-index", "SemanticEventService",
                data(DUAServiceDataClass.DERIVED_ANALYTIC),
                "Predicate-argument and event records mapped back to DUA FS/document IDs.",
                "High-throughput fact scans; expensive extraction happens at projection refresh time.");
    }

    private static DUAServiceModule geoTemporalModule() {
        return accelerator("dua-geo-temporal", "DUA Geospatial Temporal Accelerator",
                "Time ranges, coordinates, gazetteer links, movement paths, map/timeline region filters.",
                "dua-geo-temporal.spacetime-index", "GeoTemporalService",
                data(DUAServiceDataClass.DERIVED_INDEX),
                "Spatial and temporal indexes over annotation-derived facts and document metadata.",
                "Indexed range/geometry queries; core fallback is deliberately slow scan.");
    }

    private static DUAServiceModule vectorModule() {
        return accelerator("dua-vector", "DUA Vector Similarity Accelerator",
                "Embeddings, nearest neighbors, semantic deduplication, multimodal similarity, reranking candidates.",
                "dua-vector.vector-index", "VectorSimilarityService",
                data(DUAServiceDataClass.DERIVED_VECTOR),
                "Vector records keyed by stable DUA object references, never by local transient FS ids.",
                "Approximate nearest-neighbor latency; result materialization is delegated back to core.");
    }

    private static DUAServiceModule graphNavigationModule() {
        return accelerator("dua-graph-navigation", "DUA Graph Navigation Accelerator",
                "Human-friendly relationship browsing, association traversal, provenance, and inspector graph views.",
                "dua-graph-navigation.relationships", "GraphNavigationService",
                data(DUAServiceDataClass.DERIVED_GRAPH),
                "Navigation edges and relationship summaries; canonical feature payloads stay in core.",
                "Good for bounded traversals and visualization, not for raw CAS slot reads or bulk predicates.");
    }

    private static DUAServiceModule inspectorModule() {
        return module("dua-inspector", "DUA Inspector Data", DUAServiceModuleKind.INSPECTOR,
                "Composable UI view models over accelerator outputs and core materialization.",
                service("dua-inspector.view-models", "dua-inspector", "InspectorViewModelService",
                        DUAServiceModuleKind.INSPECTOR, DUAServicePerformanceClass.LOW_LATENCY,
                        protocols(DUAServiceProtocol.HTTP_OPENAPI, DUAServiceProtocol.GRPC, DUAServiceProtocol.ARROW_FLIGHT),
                        data(DUAServiceDataClass.UI_VIEW_MODEL),
                        data(DUAServiceDataClass.DERIVED_ANALYTIC),
                        List.of(required("dua-query.coordinator", "Resolve component data bindings.",
                                        DUAServiceProtocol.GRPC),
                                required(CORE_CAS_SERVICE_ID, "Materialize requested FS snippets and payload ranges.",
                                        DUAServiceProtocol.GRPC)),
                        false,
                        true,
                        "Timeline, canvas, table, graph, map, media, and custom web component data bindings.",
                        "UI latency target; caches shaped results, invalidated by projection events."));
    }

    private static DUAServiceModule governanceModule() {
        return module("dua-governance", "DUA Governance", DUAServiceModuleKind.GOVERNANCE,
                "Access control, tenant/project policy, retention, and audit decisions.",
                service("dua-governance.policy", "dua-governance", "PolicyService",
                        DUAServiceModuleKind.GOVERNANCE, DUAServicePerformanceClass.LOW_LATENCY,
                        protocols(DUAServiceProtocol.GRPC, DUAServiceProtocol.HTTP_OPENAPI),
                        data(DUAServiceDataClass.ACCESS_POLICY),
                        data(),
                        List.of(required(CORE_REGISTRY_SERVICE_ID, "Attach policy scopes to universe/corpus/document IDs.",
                                DUAServiceProtocol.GRPC)),
                        false,
                        false,
                        "Consistent authorization checks for core, query, transport, inspector, and runtime services.",
                        "Small cached decisions on hot paths; audit writes are asynchronous."));
    }

    private static DUAServiceModule observabilityModule() {
        return module("dua-observability", "DUA Observability", DUAServiceModuleKind.OBSERVABILITY,
                "Metrics, traces, logs, benchmark runs, projection lag, and storage health.",
                service("dua-observability.telemetry", "dua-observability", "TelemetryService",
                        DUAServiceModuleKind.OBSERVABILITY, DUAServicePerformanceClass.HIGH_THROUGHPUT,
                        protocols(DUAServiceProtocol.EVENT_STREAM, DUAServiceProtocol.HTTP_OPENAPI),
                        data(DUAServiceDataClass.TELEMETRY),
                        data(DUAServiceDataClass.DERIVED_ANALYTIC),
                        List.of(optional(EVENT_LOG_SERVICE_ID, "Correlate storage revisions with projection lag.",
                                DUAServiceProtocol.EVENT_STREAM)),
                        false,
                        true,
                        "Throughput, latency, memory plateau, conflict rates, projection lag, and benchmark dashboards.",
                        "Append-heavy and sampling-friendly; must never block core writes."));
    }

    private static DUAServiceModule accelerator(String moduleId, String name, String responsibility, String serviceId,
                                                String serviceName, Set<DUAServiceDataClass> derivedData,
                                                String usefulFor, String relativePerformance) {
        return module(moduleId, name, DUAServiceModuleKind.QUERY_ACCELERATOR, responsibility,
                service(serviceId, moduleId, serviceName, DUAServiceModuleKind.QUERY_ACCELERATOR,
                        DUAServicePerformanceClass.BATCH_ANALYTIC,
                        protocols(DUAServiceProtocol.GRPC, DUAServiceProtocol.ARROW_FLIGHT, DUAServiceProtocol.JDBC,
                                DUAServiceProtocol.NATIVE_DRIVER),
                        data(),
                        derivedData,
                        List.of(required(EVENT_LOG_SERVICE_ID, "Refresh projection from committed DUA revision events.",
                                        DUAServiceProtocol.EVENT_STREAM),
                                required(CORE_REGISTRY_SERVICE_ID, "Resolve stable scopes and validate readable IDs.",
                                        DUAServiceProtocol.GRPC),
                                optional(CORE_CAS_SERVICE_ID, "Backfill or materialize source payloads for rebuilds.",
                                        DUAServiceProtocol.GRPC)),
                        false,
                        true,
                        usefulFor,
                        relativePerformance));
    }

    private static DUAServiceModule module(String id, String name, DUAServiceModuleKind kind, String responsibility,
                                           DUAServiceContract... services) {
        return new DUAServiceModule(id, name, kind, responsibility, List.of(services));
    }

    private static DUAServiceContract service(String id, String moduleId, String serviceName,
                                              DUAServiceModuleKind moduleKind,
                                              DUAServicePerformanceClass performanceClass,
                                              Set<DUAServiceProtocol> protocols,
                                              Set<DUAServiceDataClass> ownedData,
                                              Set<DUAServiceDataClass> derivedData,
                                              List<DUAServiceInteraction> interactions,
                                              boolean canonicalAuthority,
                                              boolean optionalAccelerator,
                                              String usefulFor,
                                              String relativePerformance) {
        return new DUAServiceContract(id, moduleId, serviceName, moduleKind, performanceClass, protocols, ownedData,
                derivedData, interactions, canonicalAuthority, optionalAccelerator, usefulFor, relativePerformance);
    }

    private static DUAServiceInteraction required(String serviceId, String purpose, DUAServiceProtocol protocol) {
        return new DUAServiceInteraction(serviceId, purpose, protocol, true);
    }

    private static DUAServiceInteraction optional(String serviceId, String purpose, DUAServiceProtocol protocol) {
        return new DUAServiceInteraction(serviceId, purpose, protocol, false);
    }

    private static Set<DUAServiceProtocol> protocols(DUAServiceProtocol first, DUAServiceProtocol... rest) {
        EnumSet<DUAServiceProtocol> protocols = EnumSet.of(first, rest);
        return Set.copyOf(protocols);
    }

    private static Set<DUAServiceDataClass> data(DUAServiceDataClass... values) {
        if (values.length == 0) {
            return Set.of();
        }
        return Set.copyOf(EnumSet.of(values[0], values));
    }
}

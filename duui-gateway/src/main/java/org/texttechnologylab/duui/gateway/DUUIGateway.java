package org.texttechnologylab.duui.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.gateway.model.GatewayAnnotatorRegistration;
import org.texttechnologylab.duui.gateway.model.GatewayComponentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayExperimentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayPipelineDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayRunSnapshot;
import org.texttechnologylab.duui.gateway.model.GatewayServiceDefinition;
import org.texttechnologylab.duui.gateway.store.GatewayStorage;
import org.texttechnologylab.duui.gateway.validation.V1AnnotatorValidator;
import org.texttechnologylab.duui.governance.DUUIGovernor;
import org.texttechnologylab.duui.storage.DUUIStoredConfiguration;
import org.texttechnologylab.duui.storage.DUUIStoredCorpus;
import org.texttechnologylab.duui.storage.DUUIStoredDocument;
import org.texttechnologylab.duui.storage.DUUIStoredEvent;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class DUUIGateway {
    private final String id = UUID.randomUUID().toString();
    private final Instant startedAt = Instant.now();
    private final GatewayStorage storage;
    private final GatewayGovernor governor;
    private final GatewayServiceManager serviceManager;
    private final GatewayOrchestratorService orchestrators;
    private final V1AnnotatorValidator annotatorValidator;
    private final GatewayCorpusService corpusService;

    public DUUIGateway(ObjectMapper mapper) {
        this(mapper, null);
    }

    public DUUIGateway(ObjectMapper mapper, Path storagePath) {
        this.storage = storagePath == null ? new GatewayStorage() : new GatewayStorage(mapper, storagePath);
        this.governor = new GatewayGovernor(storage);
        this.serviceManager = new GatewayServiceManager(storage);
        this.orchestrators = new GatewayOrchestratorService(storage, governor, serviceManager);
        this.annotatorValidator = new V1AnnotatorValidator(mapper);
        this.corpusService = new GatewayCorpusService(storage);
        seedDefaultComposerModel();
        normalizeDefaultComposerRuntime();
    }

    public Map<String, Object> status() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", id);
        value.put("startedAt", startedAt);
        value.put("uptimeMillis", Math.max(0L, Instant.now().toEpochMilli() - startedAt.toEpochMilli()));
        value.put("runtime", ManagementFactory.getRuntimeMXBean().getName());
        value.put("annotators", storage.annotators().query().count());
        value.put("components", storage.components().query().count());
        value.put("pipelines", storage.pipelines().query().count());
        value.put("experiments", storage.experiments().query().count());
        value.put("services", storage.services().query().count());
        value.put("runs", storage.runs().query().count());
        value.put("events", storage.events().query().count());
        value.put("corpora", storage.corpora().query().count());
        value.put("documents", storage.documents().query().count());
        value.put("configurations", storage.configurations().query().count());
        value.put("liveRuns", orchestrators.liveRuns().size());
        value.put("storagePath", storage.snapshotPath().map(Path::toString).orElse("memory"));
        return value;
    }

    public Map<String, Object> dashboardStatus() {
        long uptimeMillis = Math.max(0L, Instant.now().toEpochMilli() - startedAt.toEpochMilli());
        Runtime runtime = Runtime.getRuntime();
        Map<String, Long> levels = new LinkedHashMap<>();
        Map<String, Long> types = new LinkedHashMap<>();
        for (DUUIStoredEvent event : events(10_000)) {
            levels.merge(event.level().toLowerCase(), 1L, Long::sum);
            types.merge(event.type(), 1L, Long::sum);
        }
        return Map.of(
                "session", Map.of(
                        "id", id,
                        "startedAt", startedAt,
                        "uptimeMillis", uptimeMillis,
                        "uptimeSec", uptimeMillis / 1000,
                        "pid", ProcessHandle.current().pid(),
                        "runs", storage.runs().query().count(),
                        "activeRuns", orchestrators.liveRuns().stream().map(this::dashboardRun).toList()
                ),
                "podman", Map.of(
                        "scope", "gateway-managed DUUI component registry",
                        "services", services().get("services")
                ),
                "resources", Map.of(
                        "cpu", processCpuLoad(),
                        "rss", runtime.totalMemory() - runtime.freeMemory(),
                        "systemMemoryUsed", runtime.totalMemory() - runtime.freeMemory(),
                        "systemMemoryTotal", runtime.maxMemory(),
                        "netRxBytes", 0,
                        "netTxBytes", 0
                ),
                "events", Map.of("total", storage.events().query().count(), "levels", levels, "types", types),
                "gateway", status()
        );
    }

    public GatewayAnnotatorRegistration registerAnnotator(GatewayAnnotatorRegistration request) {
        GatewayAnnotatorRegistration validated = annotatorValidator.validate(request);
        storage.annotators().put(validated.id(), validated);
        String eventId = UUID.randomUUID().toString();
        storage.events().put(eventId, new DUUIStoredEvent(
                eventId,
                Instant.now(),
                validated.errors().isEmpty() ? "INFO" : "WARN",
                "annotator.validation",
                "gateway",
                validated.id(),
                "validated DUUI v1 annotator " + validated.id(),
                Map.of("status", validated.status(), "errors", validated.errors())
        ));
        return validated;
    }

    public List<GatewayAnnotatorRegistration> annotators() {
        return storage.annotators().query().list().stream().map(entry -> entry.value()).toList();
    }

    public boolean deleteAnnotator(String id) {
        return storage.annotators().delete(id).isPresent();
    }

    public GatewayComponentDefinition putComponent(GatewayComponentDefinition component) {
        GatewayComponentDefinition normalized = new GatewayComponentDefinition(
                requireId(component.id(), "component"),
                component.name(),
                component.annotatorId(),
                component.driver(),
                component.environment(),
                component.parameters(),
                component.deployment(),
                component.sourceView(),
                component.targetView(),
                component.tags(),
                component.createdAt() == null ? Instant.now() : component.createdAt(),
                Instant.now()
        );
        storage.components().put(normalized.id(), normalized);
        putConfiguration("component", normalized.id(), Map.of("component", normalized));
        return normalized;
    }

    public GatewayPipelineDefinition putPipeline(GatewayPipelineDefinition pipeline) {
        GatewayPipelineDefinition normalized = new GatewayPipelineDefinition(
                requireId(pipeline.id(), "pipeline"),
                pipeline.name(),
                pipeline.componentIds(),
                pipeline.edges(),
                pipeline.structure(),
                pipeline.tags(),
                pipeline.createdAt() == null ? Instant.now() : pipeline.createdAt(),
                Instant.now()
        );
        storage.pipelines().put(normalized.id(), normalized);
        putConfiguration("pipeline", normalized.id(), Map.of("pipeline", normalized));
        return normalized;
    }

    public GatewayExperimentDefinition putExperiment(GatewayExperimentDefinition experiment) {
        GatewayExperimentDefinition normalized = new GatewayExperimentDefinition(
                requireId(experiment.id(), "experiment"),
                experiment.name(),
                experiment.pipelineId(),
                experiment.componentConfiguration(),
                experiment.execution(),
                experiment.flow(),
                experiment.scheduling(),
                experiment.subExperiments(),
                experiment.createdAt() == null ? Instant.now() : experiment.createdAt(),
                Instant.now()
        );
        storage.experiments().put(normalized.id(), normalized);
        putConfiguration("experiment", normalized.id(), Map.of("experiment", normalized));
        return normalized;
    }

    public Map<String, Object> composerModel() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("gateway", status());
        value.put("annotators", annotators());
        value.put("components", storage.components().query().list().stream().map(entry -> entry.value()).toList());
        value.put("pipelines", storage.pipelines().query().list().stream().map(entry -> entry.value()).toList());
        value.put("experiments", storage.experiments().query().list().stream().map(entry -> entry.value()).toList());
        value.put("serviceDefinitions", serviceManager.services());
        value.put("events", events(200));
        value.put("corpora", corpora());
        value.put("documents", documents());
        value.put("configurations", configurations());
        value.put("services", services().get("services"));
        value.put("storage", storageModel());
        value.put("orchestrator", orchestratorSurface());
        value.put("concepts", Map.of(
                "annotator", "Validated DUUI v1 annotator descriptor endpoint or deployment image.",
                "component", "DUUI-specific instance of an annotator plus driver, deployment, parameters, views, and labels.",
                "pipeline", "Structural DAG of components and checkpoints.",
                "experiment", "Runnable configuration layer over a pipeline: component params, execution, flow, scheduling, and output targets."
        ));
        value.put("capabilities", Map.of(
                "annotatorRegistration", List.of("remote", "podman", "docker", "kubernetes"),
                "validationEntrypoints", List.of("/v1/documentation", "/v1/typesystem", "/v1/communication_layer"),
                "storage", List.of("events", "annotators", "components", "pipelines", "experiments", "services", "runs", "corpora", "documents"),
                "services", List.of("declare", "inspect", "resolveEndpoints", "componentDependencies", "orchestratorManagedStart"),
                "orchestration", List.of("construct", "configure", "startRun", "stopRun", "inspect", "schedule", "dispatch", "failurePolicy", "v1Runtime", "extraServices")
        ));
        return value;
    }

    public Map<String, Object> orchestratorSurface() {
        return orchestrators.surface();
    }

    public Map<String, Object> orchestratorPlan(Map<String, Object> request) {
        return orchestrators.inspect(request);
    }

    public Map<String, Object> services() {
        List<Map<String, Object>> componentServices = components().stream().map(component -> {
            GatewayAnnotatorRegistration annotator = storage.annotators().get(component.annotatorId()).orElse(null);
            Map<String, Object> deployment = component.deployment();
            return map(
                    "id", component.id(),
                    "name", component.name(),
                    "role", component.driver(),
                    "status", annotator == null ? "unbound" : annotator.status(),
                    "endpoint", deployment.getOrDefault("endpoint", annotator == null ? "" : annotator.endpoint()),
                    "image", deployment.getOrDefault("image", annotator == null ? "" : annotator.image()),
                    "environment", component.environment(),
                    "sourceView", component.sourceView(),
                    "targetView", component.targetView(),
                    "tags", component.tags()
            );
        }).toList();
        return map(
                "services", componentServices,
                "managedServices", serviceManager.services(),
                "entrypoints", List.of(
                        "/api/gateway/annotators",
                        "/api/gateway/components",
                        "/api/gateway/pipelines",
                        "/api/gateway/experiments",
                        "/api/gateway/services",
                        "/api/gateway/runs",
                        "/api/gateway/events",
                        "/api/gateway/corpora",
                        "/api/gateway/documents"
                ),
                "orchestrators", orchestrators.liveRuns().stream().map(this::dashboardRun).toList()
        );
    }

    public List<GatewayServiceDefinition> serviceDefinitions() {
        return serviceManager.services();
    }

    public GatewayServiceDefinition putService(GatewayServiceDefinition service) {
        return serviceManager.put(service);
    }

    public Map<String, Object> inspectService(String id) {
        return serviceManager.inspect(id);
    }

    public boolean deleteService(String id) {
        return serviceManager.delete(id);
    }

    public Map<String, Object> storageModel() {
        return map(
                "events", storage.events().query().count(),
                "annotators", storage.annotators().query().count(),
                "components", storage.components().query().count(),
                "pipelines", storage.pipelines().query().count(),
                "experiments", storage.experiments().query().count(),
                "runs", storage.runs().query().count(),
                "services", storage.services().query().count(),
                "corpora", storage.corpora().query().count(),
                "documents", storage.documents().query().count(),
                "configurations", storage.configurations().query().count()
        );
    }

    public List<GatewayComponentDefinition> components() {
        return storage.components().query().list().stream().map(entry -> entry.value()).toList();
    }

    public boolean deleteComponent(String id) {
        boolean removed = storage.components().delete(id).isPresent();
        deleteConfiguration("component", id);
        return removed;
    }

    public List<GatewayPipelineDefinition> pipelines() {
        return storage.pipelines().query().list().stream().map(entry -> entry.value()).toList();
    }

    public boolean deletePipeline(String id) {
        boolean removed = storage.pipelines().delete(id).isPresent();
        deleteConfiguration("pipeline", id);
        return removed;
    }

    public List<GatewayExperimentDefinition> experiments() {
        return storage.experiments().query().list().stream().map(entry -> entry.value()).toList();
    }

    public boolean deleteExperiment(String id) {
        boolean removed = storage.experiments().delete(id).isPresent();
        deleteConfiguration("experiment", id);
        return removed;
    }

    public GatewayRunSnapshot createRun(Map<String, Object> request) {
        return orchestrators.start(request);
    }

    public GatewayRunSnapshot stopRun(String id) {
        return orchestrators.stop(id);
    }

    public boolean deleteRun(String id) {
        return storage.runs().delete(id).isPresent();
    }

    public GatewayRunSnapshot run(String id) {
        return storage.runs().require(id);
    }

    public List<DUUIStoredEvent> events(long limit) {
        var query = storage.events().query()
                .orderBy((left, right) -> right.occurredAt().compareTo(left.occurredAt()));
        if (limit > 0L) {
            query = query.limit(limit);
        }
        return query.list().stream().map(entry -> entry.value()).toList();
    }

    public List<DUUIStoredEvent> runEvents(String runId, long limit) {
        var query = storage.events().query()
                .where(event -> runId.equals(event.subjectId()))
                .orderBy((left, right) -> right.occurredAt().compareTo(left.occurredAt()));
        if (limit > 0L) {
            query = query.limit(limit);
        }
        return query.list().stream().map(entry -> entry.value()).toList();
    }

    public Map<String, Object> dashboardRun(GatewayRunSnapshot run) {
        List<DUUIStoredEvent> runEvents = runEvents(run.id(), 0);
        long started = run.startedAt() == null ? Instant.now().getEpochSecond() : run.startedAt().getEpochSecond();
        boolean terminal = List.of("completed", "failed", "stopped").contains(run.status());
        long ended = terminal ? (run.updatedAt() == null ? Instant.now().getEpochSecond() : run.updatedAt().getEpochSecond()) : 0;
        long durationMs = Math.max(0L, (run.updatedAt() == null ? Instant.now() : run.updatedAt()).toEpochMilli()
                - (run.startedAt() == null ? Instant.now() : run.startedAt()).toEpochMilli());
        List<String> stages = stringList(run.attributes().get("stages"));
        String annotator = String.valueOf(run.attributes().getOrDefault("annotator", stages.isEmpty() ? "" : stages.get(0)));
        return map(
                "id", run.id(),
                "orchestratorId", run.orchestratorId(),
                "pipeline", run.pipelineId(),
                "pipelineId", run.pipelineId(),
                "annotator", annotator,
                "stages", stages,
                "status", run.status(),
                "startedAt", started,
                "endedAt", ended == 0 ? null : ended,
                "queuedArtifacts", run.queuedArtifacts(),
                "scheduledTasks", run.scheduledTasks(),
                "completedTasks", run.completedTasks(),
                "failedTasks", run.failedTasks(),
                "durationMs", durationMs,
                "stats", map(
                        "durationMs", durationMs,
                        "resourceSamples", List.of(resourceSample(run.updatedAt(), durationMs))
                ),
                "events", runEvents.stream().sorted((left, right) -> left.occurredAt().compareTo(right.occurredAt())).map(this::dashboardEvent).toList(),
                "results", List.of(map(
                        "document", run.attributes().getOrDefault("inputMode", "gateway-run"),
                        "variant", annotator.isBlank() ? run.pipelineId() : annotator,
                        "elapsed_ms", durationMs,
                        "found", 0,
                        "metric_events", runEvents.stream().filter(event -> "metric".equalsIgnoreCase(event.level()) || event.type().contains("metric")).count(),
                        "found_text", "",
                        "failed", run.failedTasks() > 0 ? "true" : "false"
                )),
                "attributes", run.attributes()
        );
    }

    public List<Map<String, Object>> dashboardRuns() {
        return storage.runs().query()
                .orderBy((left, right) -> right.updatedAt().compareTo(left.updatedAt()))
                .list()
                .stream()
                .map(entry -> dashboardRun(entry.value()))
                .toList();
    }

    public Map<String, Object> dashboardEvent(DUUIStoredEvent event) {
        return map(
                "id", event.id(),
                "ts", event.occurredAt().getEpochSecond(),
                "time", event.occurredAt(),
                "level", event.level().toLowerCase(),
                "type", event.type(),
                "source", event.source(),
                "subjectId", event.subjectId(),
                "message", event.message(),
                "data", map("eventType", event.type(), "source", event.source(), "subjectId", event.subjectId(), "attributes", event.attributes()),
                "attributes", event.attributes()
        );
    }

    public GatewayCorpusService corpusService() {
        return corpusService;
    }

    public List<DUUIStoredCorpus> corpora() {
        return storage.corpora().query().list().stream().map(entry -> entry.value()).toList();
    }

    public List<DUUIStoredDocument> documents() {
        return storage.documents().query().list().stream().map(entry -> entry.value()).toList();
    }

    public List<DUUIStoredConfiguration> configurations() {
        return storage.configurations().query().list().stream().map(entry -> entry.value()).toList();
    }

    public GatewayStorage storage() {
        return storage;
    }

    public DUUIGovernor governor() {
        return governor;
    }

    private double processCpuLoad() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean os) {
            double value = os.getProcessCpuLoad();
            return value < 0 ? 0 : Math.round(value * 10_000.0) / 100.0;
        }
        return 0;
    }

    private Map<String, Object> resourceSample(Instant at, long durationMs) {
        Runtime runtime = Runtime.getRuntime();
        return map(
                "ts", at == null ? Instant.now().getEpochSecond() : at.getEpochSecond(),
                "cpu", processCpuLoad(),
                "rss", runtime.totalMemory() - runtime.freeMemory(),
                "netRxBps", 0,
                "netTxBps", 0,
                "durationMs", durationMs
        );
    }

    private static List<String> stringList(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().filter(Objects::nonNull).map(String::valueOf).toList();
        }
        if (value instanceof String string && !string.isBlank()) {
            return List.of(string);
        }
        return List.of();
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> value = new LinkedHashMap<>();
        for (int index = 0; index + 1 < pairs.length; index += 2) {
            value.put(String.valueOf(pairs[index]), pairs[index + 1]);
        }
        return value;
    }

    private static String requireId(String id, String kind) {
        Objects.requireNonNull(id, kind + " id");
        if (id.isBlank()) {
            throw new IllegalArgumentException(kind + " id must not be blank");
        }
        return id;
    }

    private void putConfiguration(String kind, String ownerId, Map<String, Object> payload) {
        String id = kind + ":" + ownerId + ":r1";
        storage.configurations().put(id, new DUUIStoredConfiguration(
                id,
                kind,
                ownerId,
                1,
                Instant.now(),
                Instant.now(),
                payload
        ));
    }

    private void deleteConfiguration(String kind, String ownerId) {
        storage.configurations().delete(kind + ":" + ownerId + ":r1");
    }

    private void seedDefaultComposerModel() {
        if (storage.pipelines().query().count() > 0) {
            return;
        }
        Instant now = Instant.now();
        List<GatewayAnnotatorRegistration> annotatorSeeds = List.of(
                seedAnnotator("spacy-runtime-msgpack", "spaCy runtime MsgPack", "", "podman", "localhost/duui-py-spacy-lua-msgpack:dev", "streaming"),
                seedAnnotator("taxonerd-legacy-json", "TaxoNERD legacy JSON/Lua", "", "podman", "localhost/duui-py-taxonerd-legacy:local", "legacy"),
                seedAnnotator("taxonerd-runtime-msgpack", "TaxoNERD runtime MsgPack", "", "podman", "localhost/duui-py-taxonerd-msgpack:local", "streaming"),
                seedAnnotator("taxonerd-span-windows", "TaxoNERD span windows", "", "podman", "localhost/duui-py-taxonerd-msgpack:local", "windowed"),
                seedAnnotator("taxonerd-precomputed-entities", "TaxoNERD precomputed entities", "", "podman", "localhost/duui-py-taxonerd-msgpack:local", "precomputed")
        );
        annotatorSeeds.forEach(annotator -> storage.annotators().put(annotator.id(), annotator));
        List<GatewayComponentDefinition> componentSeeds = annotatorSeeds.stream()
                .map(annotator -> new GatewayComponentDefinition(
                        annotator.id() + "-component",
                        annotator.name(),
                        annotator.id(),
                        "DUUIPodmanDriver",
                        annotator.environment(),
                        Map.of(),
                        Map.of(
                                "endpoint", "",
                                "image", annotator.image(),
                                "environment", annotator.environment(),
                                "workers", 1,
                                "scale", 1,
                                "logLevel", "TRACE",
                                "profiling", true,
                                "telemetry", true
                        ),
                        "_InitialView",
                        "_InitialView",
                        annotator.tags(),
                        now,
                        now
                ))
                .toList();
        componentSeeds.forEach(component -> storage.components().put(component.id(), component));

        String preprocess = "spacy-runtime-msgpack-component";
        List<String> branches = componentSeeds.stream()
                .map(GatewayComponentDefinition::id)
                .filter(id -> !id.equals(preprocess))
                .toList();
        GatewayPipelineDefinition pipeline = new GatewayPipelineDefinition(
                "generic-duui-pipeline",
                "Generic DUUI pipeline",
                componentSeeds.stream().map(GatewayComponentDefinition::id).toList(),
                List.of(
                        new GatewayPipelineDefinition.PipelineEdge("corpus-source", preprocess, "document-text"),
                        new GatewayPipelineDefinition.PipelineEdge(preprocess, branches.get(0), "requires-token-sentence-view"),
                        new GatewayPipelineDefinition.PipelineEdge(preprocess, branches.get(1), "requires-token-sentence-view"),
                        new GatewayPipelineDefinition.PipelineEdge(preprocess, branches.get(2), "requires-token-sentence-view"),
                        new GatewayPipelineDefinition.PipelineEdge(preprocess, branches.get(3), "requires-token-sentence-view"),
                        new GatewayPipelineDefinition.PipelineEdge(branches.get(0), "evaluation-sink", "component-output"),
                        new GatewayPipelineDefinition.PipelineEdge(branches.get(1), "evaluation-sink", "component-output"),
                        new GatewayPipelineDefinition.PipelineEdge(branches.get(2), "evaluation-sink", "component-output"),
                        new GatewayPipelineDefinition.PipelineEdge(branches.get(3), "evaluation-sink", "component-output")
                ),
                Map.of(
                        "execution", "directed-acyclic-graph",
                        "source", "selected corpus or workbench input",
                        "target", "configured output sink",
                        "levels", List.of(
                                Map.of("id", "level-1", "nodes", List.of("corpus-source")),
                                Map.of("id", "level-2", "nodes", List.of(preprocess)),
                                Map.of("id", "level-3", "nodes", branches),
                                Map.of("id", "level-4", "nodes", List.of("evaluation-sink"))
                        ),
                        "virtualNodes", Map.of(
                                "corpus-source", Map.of(
                                        "id", "corpus-source",
                                        "name", "Corpus or Workbench Input",
                                        "driver", "DUUIReader",
                                        "environment", "input",
                                        "annotatorId", "document-store",
                                        "sourceView", "_InitialView",
                                        "targetView", "_InitialView"
                                ),
                                "evaluation-sink", Map.of(
                                        "id", "evaluation-sink",
                                        "name", "Evaluation and CAS Storage",
                                        "driver", "DUUIWriter",
                                        "environment", "storage",
                                        "annotatorId", "duui-storage",
                                        "sourceView", "_InitialView",
                                        "targetView", "processed"
                                )
                        )
                ),
                List.of("seed", "dag", "descriptor-derived"),
                now,
                now
        );
        storage.pipelines().put(pipeline.id(), pipeline);
        GatewayExperimentDefinition experiment = new GatewayExperimentDefinition(
                "generic-duui-pipeline-default-experiment",
                "Default experiment",
                pipeline.id(),
                Map.of(
                        "selectedComponent", preprocess,
                        "outputTarget", "document-store",
                        "replicateRelativeDirectoryStructure", true,
                        "compression", "gzip"
                ),
                Map.of("executor", "platform", "documentMode", "single", "workers", 1, "logLevel", "TRACE", "profiling", true, "telemetry", true),
                Map.of("policy", "directed", "fanOut", "parallel-branches"),
                Map.of("policy", "direct", "maxConcurrentDocuments", 1),
                List.of(),
                now,
                now
        );
        storage.experiments().put(experiment.id(), experiment);
        putConfiguration("pipeline", pipeline.id(), Map.of("pipeline", pipeline));
        putConfiguration("experiment", experiment.id(), Map.of("experiment", experiment));
    }

    private static GatewayAnnotatorRegistration seedAnnotator(String id, String name, String endpoint, String environment, String image, String tag) {
        return new GatewayAnnotatorRegistration(
                id,
                name,
                endpoint,
                environment,
                image,
                "configured",
                Map.of(
                        "documentation", Map.of("name", name, "description", endpoint == null || endpoint.isBlank() ? image : endpoint),
                        "communicationLayer", Map.of("streaming", !"legacy".equals(tag), "contentType", "application/octet-stream"),
                        "registration", Map.of("source", "gateway-seed", "validation", "available through POST /api/gateway/annotators")
                ),
                Instant.now(),
                List.of(),
                List.of(tag)
        );
    }

    private void normalizeDefaultComposerRuntime() {
        Map<String, RuntimeDefault> defaults = Map.of(
                "spacy-runtime-msgpack", new RuntimeDefault("localhost/duui-py-spacy-lua-msgpack:dev"),
                "taxonerd-legacy-json", new RuntimeDefault("localhost/duui-py-taxonerd-legacy:local"),
                "taxonerd-runtime-msgpack", new RuntimeDefault("localhost/duui-py-taxonerd-msgpack:local"),
                "taxonerd-span-windows", new RuntimeDefault("localhost/duui-py-taxonerd-msgpack:local"),
                "taxonerd-precomputed-entities", new RuntimeDefault("localhost/duui-py-taxonerd-msgpack:local")
        );
        defaults.forEach((id, runtime) -> {
            storage.annotators().get(id)
                    .filter(annotator -> isSeedRuntime(annotator.endpoint(), annotator.image()))
                    .ifPresent(annotator -> storage.annotators().put(id, new GatewayAnnotatorRegistration(
                            annotator.id(),
                            annotator.name(),
                            "",
                            "podman",
                            runtime.image(),
                            annotator.status(),
                            annotator.descriptor(),
                            annotator.validatedAt(),
                            annotator.errors(),
                            annotator.tags()
                    )));
            String componentId = id + "-component";
            storage.components().get(componentId)
                    .filter(component -> isSeedRuntime(
                            String.valueOf(component.deployment().getOrDefault("endpoint", "")),
                            String.valueOf(component.deployment().getOrDefault("image", ""))
                    ))
                    .ifPresent(component -> {
                        Map<String, Object> deployment = new LinkedHashMap<>(component.deployment());
                        deployment.put("endpoint", "");
                        deployment.put("image", runtime.image());
                        deployment.put("environment", "podman");
                        deployment.putIfAbsent("workers", 1);
                        deployment.putIfAbsent("scale", 1);
                        deployment.putIfAbsent("logLevel", "TRACE");
                        deployment.putIfAbsent("profiling", true);
                        deployment.putIfAbsent("telemetry", true);
                        storage.components().put(componentId, new GatewayComponentDefinition(
                                component.id(),
                                component.name(),
                                component.annotatorId(),
                                "DUUIPodmanDriver",
                                "podman",
                                component.parameters(),
                                deployment,
                                component.sourceView(),
                                component.targetView(),
                                component.tags(),
                                component.createdAt(),
                                Instant.now()
                        ));
                    });
        });
        storage.experiments().get("generic-duui-pipeline-default-experiment").ifPresent(experiment -> {
            Map<String, Object> execution = new LinkedHashMap<>(experiment.execution());
            execution.putIfAbsent("logLevel", "TRACE");
            execution.putIfAbsent("profiling", true);
            execution.putIfAbsent("telemetry", true);
            GatewayExperimentDefinition normalized = new GatewayExperimentDefinition(
                    experiment.id(),
                    experiment.name(),
                    experiment.pipelineId(),
                    experiment.componentConfiguration(),
                    execution,
                    experiment.flow(),
                    experiment.scheduling(),
                    experiment.subExperiments(),
                    experiment.createdAt(),
                    Instant.now()
            );
            storage.experiments().put(normalized.id(), normalized);
            putConfiguration("experiment", normalized.id(), Map.of("experiment", normalized));
        });
        storage.pipelines().get("generic-duui-pipeline").ifPresent(pipeline -> {
            List<GatewayPipelineDefinition.PipelineEdge> edges = pipeline.edges().stream()
                    .map(edge -> new GatewayPipelineDefinition.PipelineEdge(
                            edge.from(),
                            edge.to(),
                            "taxon-candidate-output".equals(edge.checkpoint()) ? "component-output" : edge.checkpoint()
                    ))
                    .toList();
            GatewayPipelineDefinition normalized = new GatewayPipelineDefinition(
                    pipeline.id(),
                    pipeline.name(),
                    pipeline.componentIds(),
                    edges,
                    pipeline.structure(),
                    pipeline.tags(),
                    pipeline.createdAt(),
                    Instant.now()
            );
            storage.pipelines().put(normalized.id(), normalized);
            putConfiguration("pipeline", normalized.id(), Map.of("pipeline", normalized));
        });
    }

    private static boolean isSeedRuntime(String endpoint, String image) {
        return endpoint != null && (endpoint.startsWith("http://spacy") || endpoint.startsWith("http://taxonerd-") || endpoint.startsWith("http://127.0.0.1:971"))
                || List.of(
                "spacy-runtime",
                "taxonerd-legacy",
                "taxonerd-msgpack",
                "taxonerd-span",
                "taxonerd-precomputed",
                "localhost/duui-py-spacy-lua-msgpack:dev",
                "localhost/duui-py-taxonerd-legacy:local",
                "localhost/duui-py-taxonerd-msgpack:local"
        ).contains(image);
    }

    private record RuntimeDefault(String image) {
    }
}

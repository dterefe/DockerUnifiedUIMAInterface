package org.texttechnologylab.duui.gateway;

import org.apache.uima.UIMAFramework;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.CasIOUtils;
import org.apache.uima.util.XMLInputSource;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.clients.http.DUUIHttpEndpoint;
import org.texttechnologylab.duui.exception.DUUIBackoffStrategy;
import org.texttechnologylab.duui.exception.DUUIFailureAction;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.event.DUUIEvent;
import org.texttechnologylab.duui.event.DUUIEventService;
import org.texttechnologylab.duui.gateway.model.GatewayAnnotatorRegistration;
import org.texttechnologylab.duui.gateway.model.GatewayComponentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayExperimentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayPipelineDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayRunSnapshot;
import org.texttechnologylab.duui.gateway.store.GatewayStorage;
import org.texttechnologylab.duui.orchestration.DUUIOrchestrator;
import org.texttechnologylab.duui.orchestration.DUUIOrchestratorConfig;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchMode;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUITraitDirector;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutionContext;
import org.texttechnologylab.duui.pipeline.DUUICheckpoint;
import org.texttechnologylab.duui.pipeline.DUUIExecutionMode;
import org.texttechnologylab.duui.pipeline.DUUIGenerator;
import org.texttechnologylab.duui.pipeline.DUUIPipeline;
import org.texttechnologylab.duui.pipeline.DUUIStage;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Annotator;
import org.texttechnologylab.duui.protocol.v1.DUUIV1Config;
import org.texttechnologylab.duui.protocol.v1.DUUIV1TelemetryConfig;
import org.texttechnologylab.duui.storage.DUUIStoredEvent;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

public final class GatewayOrchestratorService implements AutoCloseable {
    private final GatewayStorage storage;
    private final GatewayGovernor governor;
    private final GatewayServiceManager serviceManager;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, Future<?>> liveRuns = new ConcurrentHashMap<>();

    public GatewayOrchestratorService(GatewayStorage storage, GatewayGovernor governor, GatewayServiceManager serviceManager) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.governor = Objects.requireNonNull(governor, "governor");
        this.serviceManager = Objects.requireNonNull(serviceManager, "serviceManager");
    }

    public GatewayRunSnapshot start(Map<String, Object> request) {
        String runId = UUID.randomUUID().toString();
        EffectiveRunRequest effective = effectiveRequest(request);
        Map<String, Object> attributes = new LinkedHashMap<>(request == null ? Map.of() : request);
        attributes.put("gatewayRunMode", "duui-orchestrator");
        attributes.put("orchestratorSurface", surface());
        attributes.put("resolvedPlan", inspect(request));
        GatewayRunSnapshot queued = new GatewayRunSnapshot(runId, runId, effective.pipelineId(), "queued",
                Instant.now(), Instant.now(), 0, 0, 0, 0, attributes);
        storage.runs().put(runId, queued);
        event("INFO", "run.queued", runId, "Gateway queued DUUI orchestrator run " + runId, attributes);
        liveRuns.put(runId, executor.submit(() -> run(runId, effective, request == null ? Map.of() : request)));
        return queued;
    }

    public GatewayRunSnapshot stop(String id) {
        Future<?> future = liveRuns.remove(id);
        if (future != null) {
            future.cancel(true);
        }
        GatewayRunSnapshot current = storage.runs().require(id);
        GatewayRunSnapshot stopped = new GatewayRunSnapshot(current.id(), current.orchestratorId(), current.pipelineId(), "stopped",
                current.startedAt(), Instant.now(), current.queuedArtifacts(), current.scheduledTasks(),
                current.completedTasks(), current.failedTasks(), current.attributes());
        storage.runs().put(id, stopped);
        event("WARN", "run.stopped", id, "Gateway stopped DUUI run " + id, current.attributes());
        return stopped;
    }

    public List<GatewayRunSnapshot> liveRuns() {
        return liveRuns.keySet().stream()
                .map(storage.runs()::get)
                .flatMap(java.util.Optional::stream)
                .sorted(Comparator.comparing(GatewayRunSnapshot::updatedAt).reversed())
                .toList();
    }

    public Map<String, Object> surface() {
        return map(
                "constructors", List.of(
                        "DUUIOrchestrator(DUUIPipeline)",
                        "DUUIOrchestrator(DUUIPipeline, DUUIScheduler, DUUIDirector, DUUIExecutor, DUUIOrchestratorConfig)",
                        "DUUIOrchestrator(String, DUUIPipeline, DUUIScheduler, DUUIDirector, DUUIExecutor, DUUIOrchestratorConfig)"
                ),
                "runModes", List.of("sources", "artifacts"),
                "orchestratorConfig", map("failFast", "boolean", "stopOnUnroutableArtifact", "boolean", "governor", "gateway"),
                "schedulerPolicies", List.of("firstReady"),
                "directors", List.of("DUUITraitDirector"),
                "executorDispatchModes", List.of("CALLER", "IO", "CPU", "MIXED"),
                "stageTypes", List.of("PROCESSOR", "TARGET"),
                "processorExecutionModes", List.of("LINEAR", "PARALLEL"),
                "failureActions", enumNames(DUUIFailureAction.values()),
                "backoffStrategies", enumNames(DUUIBackoffStrategy.values()),
                "v1Environments", List.of("remote", "podman", "docker", "kubernetes"),
                "v1Controls", List.of(
                        "endpoint", "image", "scale", "concurrency", "sourceView", "targetView", "parameters",
                        "streamingTransport", "contentType", "telemetry", "telemetrySampleIntervalMs", "imageFetching", "gpu", "runningAfterDestroy",
                        "timeoutSeconds", "labels", "logLevel", "profiling"
                ),
                "gatewayControls", List.of("start-run", "stop-run", "delete-run", "inspect", "events", "runs", "service-declarations"),
                "serviceDependencies", List.of("declare", "inspect", "resolve-before-component", "orchestrator-starts-runtime")
        );
    }

    public Map<String, Object> inspect(Map<String, Object> request) {
        EffectiveRunRequest effective = effectiveRequest(request);
        GatewayPipelineDefinition pipeline = storage.pipelines().require(effective.pipelineId());
        List<List<GatewayComponentDefinition>> levels = componentLevels(pipeline);
        List<Map<String, Object>> stages = new ArrayList<>();
        int index = 0;
        for (List<GatewayComponentDefinition> level : levels) {
            DUUIDispatchPolicy dispatchPolicy = dispatchPolicy(effective, level);
            DUUIFailurePolicy failurePolicy = failurePolicy(effective);
            stages.add(map(
                    "id", "stage-" + index++,
                    "type", "PROCESSOR",
                    "executionMode", executionMode(effective, level).name(),
                    "dispatchPolicy", dispatchPolicyMap(dispatchPolicy),
                    "failurePolicy", failurePolicyMap(failurePolicy),
                    "components", level.stream().map(component -> componentPlan(component, effective)).toList()
            ));
        }
        stages.add(map("id", "gateway-output", "type", "TARGET", "operation", "record-output-metric"));
        return map(
                "pipelineId", effective.pipelineId(),
                "experimentId", effective.experimentId(),
                "runMode", effective.runMode(),
                "sourceMode", effective.sourceMode(),
                "artifactCount", Math.max(1, intValue(effective.flow(), "docs", intValue(effective.request(), "docs", 1))),
                "orchestratorConfig", map(
                        "failFast", effective.failFast(),
                        "stopOnUnroutableArtifact", effective.stopOnUnroutableArtifact(),
                        "governor", "GatewayGovernor"
                ),
                "scheduler", map("policy", "firstReady", "maxInFlight", effective.parallelism()),
                "director", "DUUITraitDirector",
                "executor", map("id", "request-run-id", "dispatchMode", effective.dispatchMode().name(), "parallelism", effective.parallelism()),
                "stages", stages
        );
    }

    private void run(String runId, EffectiveRunRequest effective, Map<String, Object> request) {
        BuiltPipeline built = null;
        try {
            built = buildPipeline(effective);
            DUUIOrchestrator orchestrator = new DUUIOrchestrator(
                    runId,
                    built.pipeline(),
                    new DUUIScheduler(),
                    new DUUITraitDirector(),
                    DUUIExecutor.getInstance(runId),
                    new DUUIOrchestratorConfig(effective.failFast(), effective.stopOnUnroutableArtifact(), governor)
            );
            if ("artifacts".equals(effective.runMode())) {
                orchestrator.run(new ArrayList<DUUIArtifact<?>>(jcasArtifacts(effective, built.typeSystem())), rootContext(runId, effective));
            } else {
                orchestrator.run(rootContext(runId, effective));
            }
        } catch (Exception error) {
            GatewayRunSnapshot current = storage.runs().get(runId).orElse(new GatewayRunSnapshot(
                    runId, runId, effective.pipelineId(), "failed", Instant.now(), Instant.now(), 0, 0, 0, 1, Map.copyOf(request)));
            GatewayRunSnapshot failed = new GatewayRunSnapshot(current.id(), current.orchestratorId(), current.pipelineId(), "failed",
                    current.startedAt(), Instant.now(), current.queuedArtifacts(), current.scheduledTasks(),
                    current.completedTasks(), current.failedTasks() + 1, merge(current.attributes(), "error", error.getMessage()));
            storage.runs().put(runId, failed);
            event("ERROR", "run.failed", runId, "Gateway DUUI run failed " + runId, failed.attributes());
        } finally {
            if (built != null) {
                closeResources(runId, built.resources());
            }
            liveRuns.remove(runId);
        }
    }

    private BuiltPipeline buildPipeline(EffectiveRunRequest effective) throws Exception {
        GatewayPipelineDefinition pipelineDefinition = storage.pipelines().require(effective.pipelineId());
        DUUIPipeline.Builder builder = DUUIPipeline.builder(effective.pipelineId());
        List<AutoCloseable> resources = new ArrayList<>();
        List<List<GatewayComponentDefinition>> levels = componentLevels(pipelineDefinition);
        if (levels.isEmpty()) {
            throw new IllegalStateException("No gateway components are routable for pipeline " + effective.pipelineId());
        }

        DUUICheckpoint<JCas> current = new DUUICheckpoint<>("source");
        List<List<DUUIComponent<JCas>>> builtLevels = new ArrayList<>();
        List<TypeSystemDescription> typeSystems = new ArrayList<>();
        int index = 0;
        for (List<GatewayComponentDefinition> level : levels) {
            List<DUUIComponent<JCas>> components = new ArrayList<>();
            for (GatewayComponentDefinition component : level) {
                BuiltComponent built = jcasComponent(component, effective);
                components.add(built.component());
                typeSystems.addAll(built.typeSystems());
                resources.add(built.component());
            }
            builtLevels.add(components);
        }
        TypeSystemDescription runTypeSystem = runTypeSystem(effective, typeSystems);
        builder.source(jcasSource(effective, runTypeSystem), current);
        index = 0;
        for (List<DUUIComponent<JCas>> components : builtLevels) {
            DUUICheckpoint<JCas> next = new DUUICheckpoint<>("checkpoint-" + index);
            current.stage(DUUIStage.processor(
                    "stage-" + index,
                    executionMode(effective, levels.get(index)),
                    components,
                    next,
                    dispatchPolicy(effective, levels.get(index)),
                    failurePolicy(effective)
            ));
            builder.checkpoint(current);
            current = next;
            index++;
        }
        current.stage(DUUIStage.target("gateway-output", artifact -> {
            String id = UUID.randomUUID().toString();
            storage.events().put(id, new DUUIStoredEvent(
                    id,
                    Instant.now(),
                    "INFO",
                    "artifact.output",
                    "gateway-orchestrator",
                    effective.pipelineId(),
                    "DUUI pipeline emitted processed artifact " + artifact.id(),
                    Map.of("pipeline", effective.pipelineId(), "artifact", artifact.id())
            ));
        }));
        builder.checkpoint(current);
        return new BuiltPipeline(builder.build(), resources, runTypeSystem);
    }

    private DUUIGenerator<JCas> jcasSource(EffectiveRunRequest effective, TypeSystemDescription typeSystem) {
        return emitter -> {
            for (DUUIArtifact<JCas> artifact : jcasArtifacts(effective, typeSystem)) {
                emitter.emit(artifact);
            }
        };
    }

    private List<DUUIArtifact<JCas>> jcasArtifacts(EffectiveRunRequest effective, TypeSystemDescription typeSystem) throws Exception {
        int docs = Math.max(1, intValue(effective.flow(), "docs", intValue(effective.request(), "docs", 1)));
        List<Path> samplePaths = samplePaths(effective, docs);
        if (!samplePaths.isEmpty()) {
            List<DUUIArtifact<JCas>> artifacts = new ArrayList<>();
            for (Path path : samplePaths) {
                JCas cas = createCas(typeSystem);
                try (InputStream input = openMaybeGzip(path)) {
                    CasIOUtils.load(input, cas.getCas());
                }
                artifacts.add(DUUIArtifact.of(cas));
            }
            event("INFO", "source.xmi.loaded", effective.pipelineId(), "Loaded selected XMI artifacts for DUUI run",
                    Map.of("documents", artifacts.size(), "sampleRoot", sampleRoot().toString()));
            return artifacts;
        }
        String text = stringValue(effective.request(), "text", "");
        if (text.isBlank()) {
            text = "DUUI gateway orchestration smoke document.";
        }
        String language = stringValue(effective.request(), "language", "de");
        List<DUUIArtifact<JCas>> artifacts = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            JCas cas = createCas(typeSystem);
            cas.setDocumentLanguage(language);
            cas.setDocumentText(docs == 1 ? text : text + "\n\n[document " + (i + 1) + "]");
            artifacts.add(DUUIArtifact.of(cas));
        }
        return artifacts;
    }

    private BuiltComponent jcasComponent(GatewayComponentDefinition component, EffectiveRunRequest effective) throws Exception {
        GatewayAnnotatorRegistration annotator = storage.annotators().require(component.annotatorId());
        Map<String, Object> deployment = mergeObjects(annotator.descriptor(), component.deployment(), componentOverride(component.id(), effective));
        Map<String, String> dependencyEndpoints = ensureComponentServices(component, deployment);
        String environment = stringValue(deployment, "environment", stringValue(Map.of("environment", component.environment()), "environment", annotator.environment())).toLowerCase();
        String endpoint = stringValue(deployment, "endpoint", annotator.endpoint());
        String image = stringValue(deployment, "image", annotator.image());
        int scale = Math.max(1, intValue(deployment, "scale", 1));
        if ("remote".equals(environment) && endpoint != null && !endpoint.isBlank()) {
            List<DUUIV1Annotator> annotators = annotators(component, List.of(endpoint), component.id() + "-" + environment, effective, dependencyEndpoints);
            return new BuiltComponent(DUUIComponent.v1(component.id(), annotators), annotators.stream().map(DUUIV1Annotator::typesystem).toList());
        }
        if ("podman".equals(environment) && !image.isBlank()) {
            return podmanComponent(component, deployment, image, scale, effective, dependencyEndpoints);
        }
        if ("docker".equals(environment) && !image.isBlank()) {
            return dockerComponent(component, deployment, image, scale, effective, dependencyEndpoints);
        }
        if ("kubernetes".equals(environment) && !image.isBlank()) {
            return kubernetesComponent(component, deployment, image, scale, effective, dependencyEndpoints);
        }
        if (endpoint != null && !endpoint.isBlank()) {
            List<DUUIV1Annotator> annotators = annotators(component, List.of(endpoint), component.id() + "-" + environment + "-external", effective, dependencyEndpoints);
            return new BuiltComponent(DUUIComponent.v1(component.id(), annotators), annotators.stream().map(DUUIV1Annotator::typesystem).toList());
        }
        throw new IllegalStateException("DUUI v1 component " + component.id() + " requires an endpoint or a deployable image.");
    }

    private BuiltComponent podmanComponent(GatewayComponentDefinition component, Map<String, Object> deployment, String image, int scale, EffectiveRunRequest effective, Map<String, String> serviceEndpoints) throws Exception {
        long timeoutSeconds = Math.max(1L, longValue(deployment, "timeoutSeconds", 3600L));
        int readinessTimeoutMs = Math.max(1000, intValue(deployment, "readinessTimeoutMs", 10000));
        DUUIPodmanDriver driver = new DUUIPodmanDriver()
                .withTimeout(readinessTimeoutMs);
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIPipelineComponent pipelineComponent = new DUUIPodmanDriver.Component(image)
                .withScale(scale)
                .withWorkers(Math.max(1, intValue(deployment, "workers", 1)))
                .withImageFetching(boolValue(deployment, "imageFetching", false))
                .withGPU(boolValue(deployment, "gpu", false))
                .withRunningAfterDestroy(boolValue(deployment, "runningAfterDestroy", false))
                .withSourceView(component.sourceView())
                .withTargetView(component.targetView())
                .build()
                .withTimeout(timeoutSeconds);
        component.parameters().forEach(pipelineComponent::withParameter);
        String uuid = driver.instantiate(pipelineComponent, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        if (endpoints.isEmpty()) {
            driver.destroy(uuid);
            throw new IllegalStateException("Podman component did not expose a DUUI v1 endpoint: " + component.id());
        }
        List<DUUIV1Annotator> annotators = annotators(component, endpoints, component.id() + "-podman", effective, serviceEndpoints);
        return new BuiltComponent(
                new DUUIComponent<>(component.id(), nodes(component, annotators), () -> driver.destroy(uuid)),
                annotators.stream().map(DUUIV1Annotator::typesystem).toList()
        );
    }

    private BuiltComponent dockerComponent(GatewayComponentDefinition component, Map<String, Object> deployment, String image, int scale, EffectiveRunRequest effective, Map<String, String> serviceEndpoints) throws Exception {
        DUUIDockerDriver driver = new DUUIDockerDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIDockerDriver.Component builder = new DUUIDockerDriver.Component(image)
                .withScale(scale)
                .withWorkers(Math.max(1, intValue(deployment, "workers", 1)))
                .withImageFetching(boolValue(deployment, "imageFetching", false))
                .withGPU(boolValue(deployment, "gpu", false))
                .withRunningAfterDestroy(boolValue(deployment, "runningAfterDestroy", false))
                .withSourceView(component.sourceView())
                .withTargetView(component.targetView());
        for (String env : list(deployment.get("env"))) {
            builder.withEnv(env);
        }
        DUUIPipelineComponent pipelineComponent = builder.build()
                .withTimeout(Math.max(1L, longValue(deployment, "timeoutSeconds", 3600L)));
        component.parameters().forEach(pipelineComponent::withParameter);
        String uuid = driver.instantiate(pipelineComponent, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        if (endpoints.isEmpty()) {
            driver.destroy(uuid);
            throw new IllegalStateException("Docker component did not expose a DUUI v1 endpoint: " + component.id());
        }
        List<DUUIV1Annotator> annotators = annotators(component, endpoints, component.id() + "-docker", effective, serviceEndpoints);
        return new BuiltComponent(
                new DUUIComponent<>(component.id(), nodes(component, annotators), () -> driver.destroy(uuid)),
                annotators.stream().map(DUUIV1Annotator::typesystem).toList()
        );
    }

    private BuiltComponent kubernetesComponent(GatewayComponentDefinition component, Map<String, Object> deployment, String image, int scale, EffectiveRunRequest effective, Map<String, String> serviceEndpoints) throws Exception {
        DUUIKubernetesDriver driver = new DUUIKubernetesDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIKubernetesDriver.Component builder = new DUUIKubernetesDriver.Component(image)
                .withScale(scale)
                .withSourceView(component.sourceView())
                .withTargetView(component.targetView());
        List<String> labels = list(deployment.get("labels"));
        if (!labels.isEmpty()) {
            builder.withLabels(labels);
        }
        DUUIPipelineComponent pipelineComponent = builder.build().withTimeout(Math.max(1L, longValue(deployment, "timeoutSeconds", 3600L)));
        component.parameters().forEach(pipelineComponent::withParameter);
        String uuid = driver.instantiate(pipelineComponent, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        if (endpoints.isEmpty()) {
            driver.destroy(uuid);
            throw new IllegalStateException("Kubernetes component did not expose a DUUI v1 endpoint: " + component.id());
        }
        List<DUUIV1Annotator> annotators = annotators(component, endpoints, component.id() + "-kubernetes", effective, serviceEndpoints);
        return new BuiltComponent(
                new DUUIComponent<>(component.id(), nodes(component, annotators), () -> driver.destroy(uuid)),
                annotators.stream().map(DUUIV1Annotator::typesystem).toList()
        );
    }

    private Map<String, String> ensureComponentServices(GatewayComponentDefinition component, Map<String, Object> deployment) {
        Map<String, String> endpoints = new LinkedHashMap<>();
        endpoints.putAll(serviceManager.ensureServices(deployment.get("extraServices")));
        endpoints.putAll(serviceManager.ensureServices(deployment.get("requiresServices")));
        if (!endpoints.isEmpty()) {
            event("INFO", "component.services.ready", component.id(), "Resolved DUUI component dependency services " + component.id(), Map.of("services", endpoints));
        }
        return endpoints;
    }

    private List<DUUIV1Annotator> annotators(GatewayComponentDefinition component, List<String> endpoints, String prefix, EffectiveRunRequest effective, Map<String, String> serviceEndpoints) throws Exception {
        List<DUUIV1Annotator> annotators = new ArrayList<>();
        DUUIV1Config config = v1Config(component, effective, serviceEndpoints);
        int replica = 0;
        for (String endpoint : endpoints) {
            annotators.add(new DUUIV1Annotator(
                    prefix + "-replica-" + replica++,
                    new DUUIHttpEndpoint(URI.create(endpoint), HttpClient.newHttpClient()),
                    config
            ));
        }
        return annotators;
    }

    private List<org.texttechnologylab.duui.pipeline.component.DUUINode<JCas>> nodes(GatewayComponentDefinition component, List<DUUIV1Annotator> annotators) {
        List<org.texttechnologylab.duui.pipeline.component.DUUINode<JCas>> nodes = new ArrayList<>();
        int slot = 0;
        for (DUUIV1Annotator annotator : annotators) {
            for (int i = 0; i < annotator.config().concurrency(); i++) {
                nodes.add(org.texttechnologylab.duui.pipeline.component.DUUINode.v1(component.id() + "-slot-" + slot++, annotator));
            }
        }
        return nodes;
    }

    private DUUIV1Config v1Config(GatewayComponentDefinition component, EffectiveRunRequest effective, Map<String, String> serviceEndpoints) {
        Map<String, Object> override = componentOverride(component.id(), effective);
        Map<String, Object> deployment = mergeObjects(component.deployment(), override);
        Map<String, String> parameters = new LinkedHashMap<>(component.parameters());
        Object parameterOverride = override.get("parameters");
        if (parameterOverride instanceof Map<?, ?> map) {
            map.forEach((key, value) -> {
                if (key != null && value != null) parameters.put(String.valueOf(key), String.valueOf(value));
            });
        }
        serviceEndpoints.forEach((key, value) -> parameters.put("duui.service." + key + ".endpoint", value));
        String logLevel = stringValue(deployment, "logLevel", stringValue(effective.execution(), "logLevel", ""));
        if (!logLevel.isBlank()) {
            parameters.putIfAbsent("duui.logging.level", logLevel);
        }
        if (deployment.containsKey("profiling") || effective.execution().containsKey("profiling")) {
            parameters.putIfAbsent("duui.profiling.enabled", String.valueOf(boolValue(deployment, "profiling", boolValue(effective.execution(), "profiling", false))));
        }
        boolean telemetry = boolValue(deployment, "telemetry", boolValue(effective.execution(), "telemetry", true));
        DUUIV1TelemetryConfig telemetryConfig = telemetry
                ? new DUUIV1TelemetryConfig(
                        true,
                        intValue(deployment, "telemetryTtlMinutes", 5),
                        null,
                        intValue(deployment, "telemetrySampleIntervalMs", intValue(effective.execution(), "telemetrySampleIntervalMs", 500))
                )
                : DUUIV1TelemetryConfig.disabled();
        return new DUUIV1Config(
                Math.max(1, intValue(deployment, "concurrency", intValue(effective.execution(), "workers", 1))),
                stringValue(Map.of("sourceView", component.sourceView()), "sourceView", "_InitialView"),
                stringValue(Map.of("targetView", component.targetView()), "targetView", "_InitialView"),
                parameters,
                telemetryConfig,
                stringValue(deployment, "contentType", "application/octet-stream")
        );
    }

    private List<List<GatewayComponentDefinition>> componentLevels(GatewayPipelineDefinition pipeline) {
        Map<String, GatewayComponentDefinition> componentsById = new LinkedHashMap<>();
        for (GatewayComponentDefinition component : storage.components().query().list().stream().map(entry -> entry.value()).toList()) {
            componentsById.put(component.id(), component);
        }
        List<List<GatewayComponentDefinition>> structured = structuredLevels(pipeline, componentsById);
        if (!structured.isEmpty()) {
            return structured;
        }
        return topologicalLevels(pipeline, componentsById);
    }

    private List<List<GatewayComponentDefinition>> structuredLevels(GatewayPipelineDefinition pipeline, Map<String, GatewayComponentDefinition> componentsById) {
        Object levelsValue = pipeline.structure().get("levels");
        if (!(levelsValue instanceof List<?> levels)) {
            return List.of();
        }
        List<List<GatewayComponentDefinition>> resolved = new ArrayList<>();
        for (Object levelValue : levels) {
            if (!(levelValue instanceof Map<?, ?> level)) continue;
            List<GatewayComponentDefinition> components = new ArrayList<>();
            for (String id : list(level.get("nodes"))) {
                GatewayComponentDefinition component = componentsById.get(id);
                if (component != null) components.add(component);
            }
            if (!components.isEmpty()) resolved.add(components);
        }
        return resolved;
    }

    private List<List<GatewayComponentDefinition>> topologicalLevels(GatewayPipelineDefinition pipeline, Map<String, GatewayComponentDefinition> componentsById) {
        Set<String> componentIds = new LinkedHashSet<>(pipeline.componentIds());
        componentIds.retainAll(componentsById.keySet());
        Map<String, Integer> levelById = new LinkedHashMap<>();
        componentIds.forEach(id -> levelById.put(id, 0));
        boolean changed = true;
        int guard = 0;
        while (changed && guard++ < componentIds.size() * componentIds.size() + 1) {
            changed = false;
            for (GatewayPipelineDefinition.PipelineEdge edge : pipeline.edges()) {
                if (!componentIds.contains(edge.to())) continue;
                int nextLevel = componentIds.contains(edge.from()) ? levelById.get(edge.from()) + 1 : 0;
                if (nextLevel > levelById.get(edge.to())) {
                    levelById.put(edge.to(), nextLevel);
                    changed = true;
                }
            }
        }
        int max = levelById.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        List<List<GatewayComponentDefinition>> levels = new ArrayList<>();
        for (int index = 0; index <= max; index++) {
            int level = index;
            List<GatewayComponentDefinition> components = componentIds.stream()
                    .filter(id -> levelById.get(id) == level)
                    .map(componentsById::get)
                    .toList();
            if (!components.isEmpty()) levels.add(components);
        }
        return levels;
    }

    private EffectiveRunRequest effectiveRequest(Map<String, Object> request) {
        Map<String, Object> safeRequest = request == null ? Map.of() : request;
        String experimentId = stringValue(safeRequest, "experiment", "");
        GatewayExperimentDefinition experiment = experimentId.isBlank() ? null : storage.experiments().get(experimentId).orElse(null);
        String pipelineId = stringValue(safeRequest, "pipeline", experiment == null ? "generic-duui-pipeline" : experiment.pipelineId());
        if ((pipelineId == null || pipelineId.isBlank()) && experiment != null) {
            pipelineId = experiment.pipelineId();
        }
        Map<String, Object> execution = mergeObjects(experiment == null ? Map.of() : experiment.execution(), mapValue(safeRequest.get("execution")));
        Map<String, Object> flow = mergeObjects(experiment == null ? Map.of() : experiment.flow(), mapValue(safeRequest.get("flow")));
        Map<String, Object> scheduling = mergeObjects(experiment == null ? Map.of() : experiment.scheduling(), mapValue(safeRequest.get("scheduling")));
        Map<String, Object> componentConfiguration = mergeObjects(experiment == null ? Map.of() : experiment.componentConfiguration(), mapValue(safeRequest.get("componentConfiguration")));
        String runMode = stringValue(safeRequest, "runMode", stringValue(flow, "runMode", "sources"));
        String sourceMode = stringValue(safeRequest, "inputMode", stringValue(flow, "source", "workbench"));
        boolean failFast = boolValue(flow, "failFast", false);
        boolean stopOnUnroutable = boolValue(flow, "stopOnUnroutableArtifact", true);
        DUUIDispatchMode dispatchMode = dispatchMode(stringValue(execution, "executor", stringValue(scheduling, "dispatchMode", "mixed")));
        int parallelism = Math.max(1, intValue(scheduling, "maxConcurrentDocuments", intValue(execution, "workers", 1)));
        return new EffectiveRunRequest(
                safeRequest,
                experimentId,
                pipelineId,
                execution,
                flow,
                scheduling,
                componentConfiguration,
                runMode,
                sourceMode,
                failFast,
                stopOnUnroutable,
                dispatchMode,
                parallelism
        );
    }

    private DUUIExecutionMode executionMode(EffectiveRunRequest effective, List<GatewayComponentDefinition> level) {
        String configured = stringValue(effective.scheduling(), "stageExecutionMode", "");
        if ("linear".equalsIgnoreCase(configured)) return DUUIExecutionMode.LINEAR;
        if ("parallel".equalsIgnoreCase(configured)) return DUUIExecutionMode.PARALLEL;
        return level.size() > 1 ? DUUIExecutionMode.PARALLEL : DUUIExecutionMode.LINEAR;
    }

    private DUUIDispatchPolicy dispatchPolicy(EffectiveRunRequest effective, List<GatewayComponentDefinition> level) {
        int parallelism = Math.max(1, intValue(effective.scheduling(), "parallelism", effective.parallelism()));
        if ("caller".equalsIgnoreCase(stringValue(effective.scheduling(), "dispatchMode", ""))) {
            return DUUIDispatchPolicy.CALLER;
        }
        return DUUIDispatchPolicy.of(effective.dispatchMode(), level.size() > 1 ? Math.max(parallelism, level.size()) : parallelism);
    }

    private DUUIFailurePolicy failurePolicy(EffectiveRunRequest effective) {
        String actionValue = stringValue(effective.flow(), "failureAction", boolValue(effective.flow(), "failFast", false) ? "FAIL_FAST" : "CONTINUE");
        DUUIFailureAction action = enumValue(DUUIFailureAction.class, actionValue, DUUIFailureAction.CONTINUE);
        DUUIBackoffStrategy backoff = enumValue(DUUIBackoffStrategy.class, stringValue(effective.flow(), "backoffStrategy", "NONE"), DUUIBackoffStrategy.NONE);
        return new DUUIFailurePolicy(
                action,
                intValue(effective.flow(), "maxAttempts", 1),
                backoff,
                longValue(effective.flow(), "initialBackoffMs", 0),
                longValue(effective.flow(), "maxBackoffMs", 0),
                boolValue(effective.flow(), "jitter", false)
        );
    }

    private Map<String, Object> componentPlan(GatewayComponentDefinition component, EffectiveRunRequest effective) {
        GatewayAnnotatorRegistration annotator = storage.annotators().get(component.annotatorId()).orElse(null);
        Map<String, Object> deployment = mergeObjects(component.deployment(), componentOverride(component.id(), effective));
        return map(
                "id", component.id(),
                "name", component.name(),
                "annotatorId", component.annotatorId(),
                "driver", component.driver(),
                "environment", stringValue(deployment, "environment", component.environment()),
                "endpoint", stringValue(deployment, "endpoint", annotator == null ? "" : annotator.endpoint()),
                "image", stringValue(deployment, "image", annotator == null ? "" : annotator.image()),
                "sourceView", component.sourceView(),
                "targetView", component.targetView(),
                "scale", intValue(deployment, "scale", 1),
                "concurrency", intValue(deployment, "concurrency", intValue(effective.execution(), "workers", 1)),
                "extraServices", list(deployment.get("extraServices")),
                "requiresServices", list(deployment.get("requiresServices")),
                "parameters", component.parameters()
        );
    }

    private Map<String, Object> componentOverride(String componentId, EffectiveRunRequest effective) {
        Object value = effective.componentConfiguration().get(componentId);
        if (value instanceof Map<?, ?> map) {
            return mapValue(map);
        }
        return Map.of();
    }

    private static DUUIDispatchMode dispatchMode(String value) {
        if (value == null) return DUUIDispatchMode.MIXED;
        return switch (value.toLowerCase()) {
            case "virtual", "io", "virtual-thread-fanout" -> DUUIDispatchMode.IO;
            case "platform", "cpu", "platform-worker-pool" -> DUUIDispatchMode.CPU;
            default -> DUUIDispatchMode.MIXED;
        };
    }

    private static Map<String, Object> dispatchPolicyMap(DUUIDispatchPolicy policy) {
        return map("caller", policy.caller(), "mode", policy.mode() == null ? "INHERIT" : policy.mode().name(), "parallelism", policy.parallelism());
    }

    private static Map<String, Object> failurePolicyMap(DUUIFailurePolicy policy) {
        return map(
                "action", policy.action().name(),
                "maxAttempts", policy.maxAttempts(),
                "backoffStrategy", policy.backoffStrategy().name(),
                "initialBackoffMs", policy.initialBackoffMs(),
                "maxBackoffMs", policy.maxBackoffMs(),
                "jitter", policy.jitter()
        );
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, String value, E fallback) {
        if (value == null || value.isBlank()) return fallback;
        try {
            return Enum.valueOf(type, value.trim().toUpperCase().replace('-', '_'));
        } catch (IllegalArgumentException ignored) {
            return fallback;
        }
    }

    private void closeResources(String runId, List<AutoCloseable> resources) {
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception error) {
                event("WARN", "resource.close.failed", runId, "Failed to close DUUI component resource", Map.of("error", error.getMessage()));
            }
        }
    }

    private DUUIExecutionContext rootContext(String runId, EffectiveRunRequest effective) {
        DUUIEventService eventService = new DUUIEventService(List.of(event -> storeDuuiEvent(runId, effective, event)));
        return new DUUIExecutionContext()
                .eventContext(org.texttechnologylab.duui.event.DUUIEventContext.root(runId, null))
                .eventService(eventService);
    }

    private void storeDuuiEvent(String runId, EffectiveRunRequest effective, DUUIEvent event) {
        if (event == null) return;
        String level = event.level() == null
                ? ("METRIC".equals(event.type().name()) ? "METRIC" : "INFO")
                : event.level().name();
        String eventType = "duui." + event.type().name().toLowerCase() + (event.name() == null || event.name().isBlank() ? "" : "." + event.name());
        String subject = firstPresent(event.orchestratorId(), runId, effective.pipelineId());
        Map<String, Object> attributes = new LinkedHashMap<>();
        put(attributes, "run", runId);
        put(attributes, "pipeline", effective.pipelineId());
        put(attributes, "duuiEventId", event.id());
        put(attributes, "duuiEventType", event.type().name());
        put(attributes, "eventName", event.name());
        put(attributes, "status", event.status() == null ? "" : event.status().name());
        put(attributes, "traceId", event.traceId());
        put(attributes, "spanId", event.spanId());
        put(attributes, "parentSpanId", event.parentSpanId());
        put(attributes, "taskId", event.taskId());
        put(attributes, "artifactId", event.artifactId());
        put(attributes, "checkpointId", event.checkpointId());
        put(attributes, "stageId", event.stageId());
        put(attributes, "componentId", event.componentId());
        put(attributes, "nodeId", event.nodeId());
        put(attributes, "annotatorId", event.annotatorId());
        put(attributes, "workerId", event.workerId());
        if (event.metricName() != null) {
            attributes.put("metric", map(
                    "name", event.metricName(),
                    "value", event.metricValue(),
                    "unit", event.metricUnit(),
                    "intervalMs", event.metricIntervalMs(),
                    "tags", event.metricTags()
            ));
        }
        if (event.errorType() != null || event.stackTrace() != null || event.recoveryHint() != null) {
            attributes.put("error", map(
                    "type", event.errorType(),
                    "stackTrace", event.stackTrace(),
                    "recoveryHint", event.recoveryHint()
            ));
        }
        attributes.putAll(event.attributes());
        storage.events().put(event.id(), new DUUIStoredEvent(
                event.id(),
                event.timestamp(),
                level,
                eventType,
                event.name() == null || event.name().isBlank() ? "duui" : event.name(),
                subject,
                firstPresent(event.message(), event.metricName(), eventType),
                attributes
        ));
    }

    private TypeSystemDescription runTypeSystem(EffectiveRunRequest effective, List<TypeSystemDescription> componentTypeSystems) throws Exception {
        List<TypeSystemDescription> descriptions = new ArrayList<>();
        descriptions.add(TypeSystemDescriptionFactory.createTypeSystemDescription());
        TypeSystemDescription sampleTypeSystem = sampleTypeSystem();
        if (sampleTypeSystem != null) descriptions.add(sampleTypeSystem);
        if (componentTypeSystems != null) {
            componentTypeSystems.stream().filter(Objects::nonNull).forEach(descriptions::add);
        }
        TypeSystemDescription merged = CasCreationUtils.mergeTypeSystems(descriptions);
        event("DEBUG", "typesystem.merged", effective.pipelineId(), "Merged DUUI run type system",
                Map.of("descriptions", descriptions.size(), "pipeline", effective.pipelineId()));
        return merged;
    }

    private TypeSystemDescription sampleTypeSystem() {
        Path root = sampleRoot();
        Path gzip = root.resolve("TypeSystem.xml.gz");
        Path xml = root.resolve("TypeSystem.xml");
        Path file = Files.isRegularFile(gzip) ? gzip : xml;
        if (!Files.isRegularFile(file)) return null;
        try (InputStream input = openMaybeGzip(file)) {
            return UIMAFramework.getXMLParser().parseTypeSystemDescription(new XMLInputSource(input, null));
        } catch (Exception error) {
            event("WARN", "typesystem.sample.failed", "gateway-source", "Failed to parse sample corpus TypeSystem", Map.of("file", file.toString(), "error", error.getMessage()));
            return null;
        }
    }

    private static JCas createCas(TypeSystemDescription typeSystem) throws Exception {
        if (typeSystem == null) {
            return JCasFactory.createJCas();
        }
        return JCasFactory.createJCas(typeSystem);
    }

    private List<Path> samplePaths(EffectiveRunRequest effective, int limit) {
        List<String> selected = new ArrayList<>();
        selected.addAll(list(effective.request().get("samples")));
        selected.addAll(list(effective.request().get("selectedCorpusPaths")));
        boolean wantsCorpus = "xmi".equalsIgnoreCase(effective.sourceMode())
                || "selected-corpus-artifacts".equalsIgnoreCase(effective.sourceMode())
                || "xmi".equalsIgnoreCase(stringValue(effective.request(), "inputMode", ""));
        if (selected.isEmpty() && !wantsCorpus) {
            return List.of();
        }
        Path root = sampleRoot();
        List<Path> paths = new ArrayList<>();
        if (selected.isEmpty()) {
            collectXmi(root, paths, limit);
            return paths;
        }
        for (String raw : selected) {
            if (paths.size() >= limit) break;
            String safe = raw == null ? "" : raw.replace('\\', '/');
            while (safe.startsWith("/")) safe = safe.substring(1);
            Path path = safe.isBlank() ? root : root.resolve(safe).normalize();
            if (!path.startsWith(root) || !Files.exists(path)) continue;
            collectXmi(path, paths, limit);
        }
        return paths.stream().distinct().limit(Math.max(1, limit)).toList();
    }

    private static void collectXmi(Path path, List<Path> out, int limit) {
        if (out.size() >= limit || path == null || !Files.exists(path)) return;
        if (Files.isRegularFile(path) && isXmi(path)) {
            out.add(path);
            return;
        }
        if (!Files.isDirectory(path)) return;
        try (var stream = Files.walk(path)) {
            stream
                    .filter(Files::isRegularFile)
                    .filter(GatewayOrchestratorService::isXmi)
                    .sorted(Comparator.comparing(Path::toString))
                    .limit(Math.max(1, limit) - out.size())
                    .forEach(out::add);
        } catch (Exception ignored) {
        }
    }

    private static boolean isXmi(Path path) {
        String name = path.getFileName().toString();
        return name.endsWith(".xmi") || name.endsWith(".xmi.gz");
    }

    private static InputStream openMaybeGzip(Path path) throws Exception {
        InputStream input = Files.newInputStream(path);
        if (path.getFileName().toString().endsWith(".gz")) {
            return new GZIPInputStream(input);
        }
        return input;
    }

    private static Path sampleRoot() {
        return Path.of(System.getProperty(
                "duui.gateway.sampleRoot",
                env("DUUI_GATEWAY_SAMPLE_ROOT", "/home/stud_homes/s0424382/projects/ttlab/uce/biofid-nova-preprocessing/preprocessed_corpora_sample_1000")
        )).toAbsolutePath().normalize();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static JCas healthCas() throws Exception {
        JCas cas = JCasFactory.createJCas();
        cas.setDocumentLanguage("en");
        cas.setDocumentText("DUUI health check.");
        return cas;
    }

    private static List<String> list(Object value) {
        if (value instanceof List<?> values) {
            return values.stream().filter(Objects::nonNull).map(String::valueOf).toList();
        }
        if (value instanceof String text && !text.isBlank()) {
            return List.of(text);
        }
        return List.of();
    }

    private static List<String> listOrDefault(Object value, List<String> fallback) {
        List<String> list = list(value);
        return list.isEmpty() ? fallback : list;
    }

    private static Map<String, Object> mapValue(Object value) {
        if (!(value instanceof Map<?, ?> input)) return Map.of();
        Map<String, Object> result = new LinkedHashMap<>();
        input.forEach((key, item) -> {
            if (key != null) result.put(String.valueOf(key), item);
        });
        return result;
    }

    private static Map<String, Object> mergeObjects(Map<?, ?>... maps) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (maps == null) return result;
        for (Map<?, ?> map : maps) {
            if (map == null) continue;
            map.forEach((key, value) -> {
                if (key != null && value != null) result.put(String.valueOf(key), value);
            });
        }
        return result;
    }

    private static String stringValue(Map<?, ?> map, String key, String fallback) {
        Object value = map == null ? null : map.get(key);
        if (value == null) return fallback;
        String text = String.valueOf(value);
        return text.isBlank() ? fallback : text;
    }

    private static int intValue(Map<?, ?> map, String key, int fallback) {
        Object value = map == null ? null : map.get(key);
        if (value instanceof Number number) return number.intValue();
        if (value != null) {
            try {
                return Integer.parseInt(String.valueOf(value));
            } catch (NumberFormatException ignored) {
            }
        }
        return fallback;
    }

    private static long longValue(Map<?, ?> map, String key, long fallback) {
        Object value = map == null ? null : map.get(key);
        if (value instanceof Number number) return number.longValue();
        if (value != null) {
            try {
                return Long.parseLong(String.valueOf(value));
            } catch (NumberFormatException ignored) {
            }
        }
        return fallback;
    }

    private static boolean boolValue(Map<?, ?> map, String key, boolean fallback) {
        Object value = map == null ? null : map.get(key);
        if (value instanceof Boolean bool) return bool;
        if (value != null) return Boolean.parseBoolean(String.valueOf(value));
        return fallback;
    }

    private static Map<String, Object> merge(Map<String, Object> attributes, Object... values) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (attributes != null) {
            merged.putAll(attributes);
        }
        for (int index = 0; index + 1 < values.length; index += 2) {
            if (values[index + 1] != null) {
                merged.put(String.valueOf(values[index]), values[index + 1]);
            }
        }
        return Map.copyOf(merged);
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> value = new LinkedHashMap<>();
        for (int index = 0; index + 1 < pairs.length; index += 2) {
            if (pairs[index + 1] != null) {
                value.put(String.valueOf(pairs[index]), pairs[index + 1]);
            }
        }
        return value;
    }

    private static void put(Map<String, Object> target, String key, Object value) {
        if (key != null && value != null) {
            target.put(key, value);
        }
    }

    private static List<String> enumNames(Enum<?>[] values) {
        return java.util.Arrays.stream(values).map(Enum::name).toList();
    }

    private static String firstPresent(String... values) {
        if (values == null) return "";
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return "";
    }

    private void event(String level, String type, String subjectId, String message, Map<String, Object> attributes) {
        String id = UUID.randomUUID().toString();
        storage.events().put(id, new DUUIStoredEvent(id, Instant.now(), level, type, "gateway-orchestrator", subjectId, message, attributes));
    }

    @Override
    public void close() {
        liveRuns.values().forEach(future -> future.cancel(true));
        executor.shutdownNow();
    }

    private record BuiltPipeline(DUUIPipeline pipeline, List<AutoCloseable> resources, TypeSystemDescription typeSystem) {
    }

    private record BuiltComponent(DUUIComponent<JCas> component, List<TypeSystemDescription> typeSystems) {
    }

    private record EffectiveRunRequest(
            Map<String, Object> request,
            String experimentId,
            String pipelineId,
            Map<String, Object> execution,
            Map<String, Object> flow,
            Map<String, Object> scheduling,
            Map<String, Object> componentConfiguration,
            String runMode,
            String sourceMode,
            boolean failFast,
            boolean stopOnUnroutableArtifact,
            DUUIDispatchMode dispatchMode,
            int parallelism
    ) {
    }
}

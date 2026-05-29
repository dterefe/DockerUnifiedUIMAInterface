package org.texttechnologylab.duui.gateway;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPodmanDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.duui.gateway.model.GatewayServiceDefinition;
import org.texttechnologylab.duui.gateway.store.GatewayStorage;
import org.texttechnologylab.duui.storage.DUUIStoredEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class GatewayServiceManager implements AutoCloseable {
    private final GatewayStorage storage;
    private final Map<String, RuntimeService> runtimes = new ConcurrentHashMap<>();

    public GatewayServiceManager(GatewayStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    public List<GatewayServiceDefinition> services() {
        return storage.services().query()
                .orderBy((left, right) -> right.updatedAt().compareTo(left.updatedAt()))
                .list()
                .stream()
                .map(entry -> entry.value())
                .toList();
    }

    public GatewayServiceDefinition put(GatewayServiceDefinition service) {
        Instant now = Instant.now();
        GatewayServiceDefinition normalized = new GatewayServiceDefinition(
                requireId(service.id(), "service"),
                service.name(),
                blankDefault(service.kind(), "auxiliary"),
                blankDefault(service.environment(), "remote"),
                nullToBlank(service.image()),
                nullToBlank(service.endpoint()),
                blankDefault(service.status(), "registered"),
                service.scale(),
                service.workers(),
                service.parameters(),
                service.deployment(),
                service.endpoints(),
                service.tags(),
                service.createdAt() == null ? now : service.createdAt(),
                now,
                service.startedAt(),
                service.runtime()
        );
        storage.services().put(normalized.id(), normalized);
        event("INFO", "service.registered", normalized.id(), "Registered DUUI gateway service " + normalized.id(), Map.of("environment", normalized.environment()));
        return normalized;
    }

    public boolean delete(String id) {
        if (runtimes.containsKey(id)) {
            stop(id);
        }
        return storage.services().delete(id).isPresent();
    }

    public GatewayServiceDefinition start(String id) {
        GatewayServiceDefinition service = storage.services().require(id);
        RuntimeService runtime = runtimes.get(id);
        if (runtime != null) {
            return copy(service, "running", runtime.endpoints(), merge(service.runtime(),
                    "managed", true,
                    "uuid", runtime.uuid(),
                    "endpoints", runtime.endpoints()
            ), runtime.startedAt());
        }
        String environment = service.environment().toLowerCase();
        if ("running".equalsIgnoreCase(service.status()) && !List.of("podman", "docker", "kubernetes").contains(environment)) {
            return service;
        }
        if ("running".equalsIgnoreCase(service.status()) && !boolValue(service.runtime(), "managed", true)) {
            return service;
        }
        try {
            GatewayServiceDefinition running = switch (environment) {
                case "podman" -> startPodman(service);
                case "docker" -> startDocker(service);
                case "kubernetes" -> startKubernetes(service);
                case "remote" -> markExternalRunning(service, "remote");
                default -> throw new IllegalArgumentException("Unsupported service environment: " + environment);
            };
            storage.services().put(id, running);
            event("INFO", "service.started", id, "Started DUUI gateway service " + id, running.runtime());
            return running;
        } catch (Exception error) {
            GatewayServiceDefinition failed = copy(service, "failed", List.of(), merge(service.runtime(), "error", error.getMessage()), service.startedAt());
            storage.services().put(id, failed);
            event("ERROR", "service.start.failed", id, "Failed to start DUUI gateway service " + id, failed.runtime());
            throw new IllegalStateException("Failed to start gateway service " + id, error);
        }
    }

    public GatewayServiceDefinition stop(String id) {
        GatewayServiceDefinition current = storage.services().require(id);
        RuntimeService runtime = runtimes.remove(id);
        if (runtime != null) {
            try {
                runtime.driver().destroy(runtime.uuid());
            } catch (Exception error) {
                event("WARN", "service.stop.failed", id, "Failed to stop DUUI gateway service " + id, Map.of("error", error.getMessage()));
            }
        }
        GatewayServiceDefinition stopped = copy(current, "stopped", current.endpoints(), merge(current.runtime(), "stoppedAt", Instant.now().toString()), current.startedAt());
        storage.services().put(id, stopped);
        event("WARN", "service.stopped", id, "Stopped DUUI gateway service " + id, stopped.runtime());
        return stopped;
    }

    public GatewayServiceDefinition restart(String id) {
        stop(id);
        return start(id);
    }

    public Map<String, Object> inspect(String id) {
        GatewayServiceDefinition service = storage.services().require(id);
        RuntimeService runtime = runtimes.get(id);
        return map(
                "definition", service,
                "runtime", runtime == null ? service.runtime() : map(
                        "uuid", runtime.uuid(),
                        "endpoints", runtime.endpoints(),
                        "startedAt", runtime.startedAt(),
                        "managed", true
                ),
                "actions", List.of("declare", "inspect", "resolveEndpoints", "orchestratorEnsure"),
                "resolvedEndpoints", resolvedEndpoints(id)
        );
    }

    public List<String> resolvedEndpoints(String id) {
        RuntimeService runtime = runtimes.get(id);
        if (runtime != null) return runtime.endpoints();
        GatewayServiceDefinition service = storage.services().get(id).orElse(null);
        if (service == null) return List.of();
        if (!service.endpoints().isEmpty()) return service.endpoints();
        if (service.endpoint() != null && !service.endpoint().isBlank()) return List.of(service.endpoint());
        return List.of();
    }

    public Map<String, String> ensureServices(Object value) {
        Map<String, String> endpoints = new LinkedHashMap<>();
        for (String id : ids(value)) {
            GatewayServiceDefinition running = start(id);
            List<String> resolved = resolvedEndpoints(running.id());
            if (!resolved.isEmpty()) {
                endpoints.put(running.id(), resolved.get(0));
            }
        }
        return endpoints;
    }

    private GatewayServiceDefinition markExternalRunning(GatewayServiceDefinition service, String mode) {
        if (service.endpoint() == null || service.endpoint().isBlank()) {
            throw new IllegalArgumentException("Remote service requires endpoint: " + service.id());
        }
        return copy(service, "running", List.of(service.endpoint()), merge(service.runtime(), "mode", mode, "managed", false), Instant.now());
    }

    private GatewayServiceDefinition startPodman(GatewayServiceDefinition service) throws Exception {
        if (service.image() == null || service.image().isBlank()) {
            if (service.endpoint() != null && !service.endpoint().isBlank()) {
                return markExternalRunning(service, "podman-external");
            }
            throw new IllegalArgumentException("Podman service requires image or endpoint: " + service.id());
        }
        DUUIPodmanDriver driver = new DUUIPodmanDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIPipelineComponent component = new DUUIPodmanDriver.Component(service.image())
                .withName(service.name())
                .withScale(service.scale())
                .withWorkers(service.workers())
                .withImageFetching(boolValue(service.deployment(), "imageFetching", false))
                .withGPU(boolValue(service.deployment(), "gpu", false))
                .withRunningAfterDestroy(boolValue(service.deployment(), "runningAfterDestroy", false))
                .withSourceView(stringValue(service.deployment(), "sourceView", "_InitialView"))
                .withTargetView(stringValue(service.deployment(), "targetView", "_InitialView"))
                .build()
                .withTimeout(longValue(service.deployment(), "timeoutSeconds", 3600));
        for (String env : ids(service.deployment().get("env"))) {
            component.withEnv(env);
        }
        service.parameters().forEach(component::withParameter);
        String uuid = driver.instantiate(component, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        RuntimeService runtime = new RuntimeService(driver, uuid, endpoints, Instant.now());
        runtimes.put(service.id(), runtime);
        return copy(service, "running", endpoints, merge(service.runtime(),
                "mode", "podman",
                "managed", true,
                "uuid", uuid,
                "endpoints", endpoints
        ), runtime.startedAt());
    }

    private GatewayServiceDefinition startDocker(GatewayServiceDefinition service) throws Exception {
        if (service.image() == null || service.image().isBlank()) {
            if (service.endpoint() != null && !service.endpoint().isBlank()) {
                return markExternalRunning(service, "docker-external");
            }
            throw new IllegalArgumentException("Docker service requires image or endpoint: " + service.id());
        }
        DUUIDockerDriver driver = new DUUIDockerDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIDockerDriver.Component builder = new DUUIDockerDriver.Component(service.image())
                .withName(service.name())
                .withScale(service.scale())
                .withWorkers(service.workers())
                .withImageFetching(boolValue(service.deployment(), "imageFetching", false))
                .withGPU(boolValue(service.deployment(), "gpu", false))
                .withRunningAfterDestroy(boolValue(service.deployment(), "runningAfterDestroy", false))
                .withSourceView(stringValue(service.deployment(), "sourceView", "_InitialView"))
                .withTargetView(stringValue(service.deployment(), "targetView", "_InitialView"));
        for (String env : ids(service.deployment().get("env"))) {
            builder.withEnv(env);
        }
        DUUIPipelineComponent component = builder.build().withTimeout(longValue(service.deployment(), "timeoutSeconds", 3600));
        service.parameters().forEach(component::withParameter);
        String uuid = driver.instantiate(component, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        RuntimeService runtime = new RuntimeService(driver, uuid, endpoints, Instant.now());
        runtimes.put(service.id(), runtime);
        return copy(service, "running", endpoints, merge(service.runtime(),
                "mode", "docker",
                "managed", true,
                "uuid", uuid,
                "endpoints", endpoints
        ), runtime.startedAt());
    }

    private GatewayServiceDefinition startKubernetes(GatewayServiceDefinition service) throws Exception {
        if (service.image() == null || service.image().isBlank()) {
            if (service.endpoint() != null && !service.endpoint().isBlank()) {
                return markExternalRunning(service, "kubernetes-external");
            }
            throw new IllegalArgumentException("Kubernetes service requires image or endpoint: " + service.id());
        }
        DUUIKubernetesDriver driver = new DUUIKubernetesDriver();
        driver.setLuaContext(LuaConsts.getJSON());
        DUUIKubernetesDriver.Component builder = new DUUIKubernetesDriver.Component(service.image())
                .withName(service.name())
                .withScale(service.scale())
                .withSourceView(stringValue(service.deployment(), "sourceView", "_InitialView"))
                .withTargetView(stringValue(service.deployment(), "targetView", "_InitialView"));
        List<String> labels = ids(service.deployment().get("labels"));
        if (!labels.isEmpty()) {
            builder.withLabels(labels);
        }
        DUUIPipelineComponent component = builder.build().withTimeout(longValue(service.deployment(), "timeoutSeconds", 3600));
        service.parameters().forEach(component::withParameter);
        String uuid = driver.instantiate(component, healthCas(), true, new AtomicBoolean(false));
        List<String> endpoints = driver.getEndpointUrls(uuid);
        RuntimeService runtime = new RuntimeService(driver, uuid, endpoints, Instant.now());
        runtimes.put(service.id(), runtime);
        return copy(service, "running", endpoints, merge(service.runtime(),
                "mode", "kubernetes",
                "managed", true,
                "uuid", uuid,
                "endpoints", endpoints
        ), runtime.startedAt());
    }

    private GatewayServiceDefinition copy(GatewayServiceDefinition source, String status, List<String> endpoints, Map<String, Object> runtime, Instant startedAt) {
        return new GatewayServiceDefinition(
                source.id(),
                source.name(),
                source.kind(),
                source.environment(),
                source.image(),
                source.endpoint(),
                status,
                source.scale(),
                source.workers(),
                source.parameters(),
                source.deployment(),
                endpoints,
                source.tags(),
                source.createdAt(),
                Instant.now(),
                startedAt,
                runtime
        );
    }

    private static JCas healthCas() throws Exception {
        JCas cas = JCasFactory.createJCas();
        cas.setDocumentLanguage("en");
        cas.setDocumentText("DUUI gateway service health check.");
        return cas;
    }

    private static List<String> ids(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().filter(Objects::nonNull).map(String::valueOf).filter(text -> !text.isBlank()).toList();
        }
        if (value instanceof String text && !text.isBlank()) {
            return List.of(text);
        }
        if (value instanceof Map<?, ?> map) {
            Object id = map.get("id");
            return id == null ? List.of() : List.of(String.valueOf(id));
        }
        return List.of();
    }

    private static Map<String, Object> merge(Map<String, Object> attributes, Object... values) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (attributes != null) merged.putAll(attributes);
        for (int index = 0; index + 1 < values.length; index += 2) {
            if (values[index + 1] != null) {
                merged.put(String.valueOf(values[index]), values[index + 1]);
            }
        }
        return Map.copyOf(merged);
    }

    private static String requireId(String id, String kind) {
        Objects.requireNonNull(id, kind + " id");
        if (id.isBlank()) throw new IllegalArgumentException(kind + " id must not be blank");
        return id;
    }

    private static String blankDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String nullToBlank(String value) {
        return value == null ? "" : value;
    }

    private static String stringValue(Map<String, Object> map, String key, String fallback) {
        Object value = map == null ? null : map.get(key);
        return value == null || String.valueOf(value).isBlank() ? fallback : String.valueOf(value);
    }

    private static long longValue(Map<String, Object> map, String key, long fallback) {
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

    private static boolean boolValue(Map<String, Object> map, String key, boolean fallback) {
        Object value = map == null ? null : map.get(key);
        if (value instanceof Boolean bool) return bool;
        return value == null ? fallback : Boolean.parseBoolean(String.valueOf(value));
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> value = new LinkedHashMap<>();
        for (int index = 0; index + 1 < pairs.length; index += 2) {
            value.put(String.valueOf(pairs[index]), pairs[index + 1]);
        }
        return value;
    }

    private void event(String level, String type, String subjectId, String message, Map<String, Object> attributes) {
        String id = UUID.randomUUID().toString();
        storage.events().put(id, new DUUIStoredEvent(id, Instant.now(), level, type, "gateway-services", subjectId, message, attributes));
    }

    @Override
    public void close() {
        for (String id : new ArrayList<>(runtimes.keySet())) {
            stop(id);
        }
    }

    private record RuntimeService(IDUUIDriverInterface driver, String uuid, List<String> endpoints, Instant startedAt) {
    }
}

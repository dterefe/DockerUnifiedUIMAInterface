package org.texttechnologylab.duui.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.texttechnologylab.duui.gateway.model.GatewayAnnotatorRegistration;
import org.texttechnologylab.duui.gateway.model.GatewayComponentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayExperimentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayPipelineDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayServiceDefinition;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public final class DUUIGatewayServer implements AutoCloseable {
    private final DUUIGateway gateway;
    private final ObjectMapper mapper;
    private final Path dashboardRoot;
    private final HttpServer server;

    public DUUIGatewayServer(DUUIGateway gateway, ObjectMapper mapper, String host, int port, Path dashboardRoot) throws IOException {
        this.gateway = gateway;
        this.mapper = mapper;
        this.dashboardRoot = dashboardRoot;
        this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.server.createContext("/api/gateway/status", this::status);
        this.server.createContext("/api/gateway/composer", this::composer);
        this.server.createContext("/api/gateway/annotators", this::annotators);
        this.server.createContext("/api/gateway/components", this::components);
        this.server.createContext("/api/gateway/pipelines", this::pipelines);
        this.server.createContext("/api/gateway/experiments", this::experiments);
        this.server.createContext("/api/gateway/runs", this::runs);
        this.server.createContext("/api/gateway/orchestrator", this::orchestrator);
        this.server.createContext("/api/gateway/events", this::events);
        this.server.createContext("/api/gateway/services", this::services);
        this.server.createContext("/api/gateway/storage", this::storage);
        this.server.createContext("/api/gateway/configurations", this::configurations);
        this.server.createContext("/api/gateway/corpora", this::corpora);
        this.server.createContext("/api/gateway/documents", this::documents);
        this.server.createContext("/api/dashboard/status", this::dashboardStatus);
        this.server.createContext("/api/pipelines", this::workbenchPipelines);
        this.server.createContext("/api/annotators", this::workbenchAnnotators);
        this.server.createContext("/api/stages", this::workbenchStages);
        this.server.createContext("/api/samples", this::emptyList);
        this.server.createContext("/api/corpus/artifact", this::corpusArtifact);
        this.server.createContext("/api/corpus/selection", this::corpusSelection);
        this.server.createContext("/api/corpus/typesystem", this::corpusTypesystem);
        this.server.createContext("/api/corpus/playground-model", this::playgroundModel);
        this.server.createContext("/api/corpus", this::corpus);
        this.server.createContext("/api/runs", this::workbenchRuns);
        this.server.createContext("/", this::staticAsset);
    }

    public void start() {
        server.start();
    }

    private void status(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.status());
    }

    private void annotators(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/annotators");
        if (!suffix.isBlank()) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                json(exchange, gateway.deleteAnnotator(suffix) ? 200 : 404, Map.of("deleted", suffix));
                return;
            }
            methodNotAllowed(exchange);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.annotators());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            GatewayAnnotatorRegistration request = mapper.readValue(exchange.getRequestBody(), GatewayAnnotatorRegistration.class);
            json(exchange, 201, gateway.registerAnnotator(request));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void composer(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.composerModel());
    }

    private void components(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/components");
        if (!suffix.isBlank()) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                json(exchange, gateway.deleteComponent(suffix) ? 200 : 404, Map.of("deleted", suffix));
                return;
            }
            methodNotAllowed(exchange);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.components());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 201, gateway.putComponent(mapper.readValue(exchange.getRequestBody(), GatewayComponentDefinition.class)));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void pipelines(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/pipelines");
        if (!suffix.isBlank()) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                json(exchange, gateway.deletePipeline(suffix) ? 200 : 404, Map.of("deleted", suffix));
                return;
            }
            methodNotAllowed(exchange);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.pipelines());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 201, gateway.putPipeline(mapper.readValue(exchange.getRequestBody(), GatewayPipelineDefinition.class)));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void experiments(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/experiments");
        if (!suffix.isBlank()) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                json(exchange, gateway.deleteExperiment(suffix) ? 200 : 404, Map.of("deleted", suffix));
                return;
            }
            methodNotAllowed(exchange);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.experiments());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 201, gateway.putExperiment(mapper.readValue(exchange.getRequestBody(), GatewayExperimentDefinition.class)));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void runs(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/runs");
        if (!suffix.isBlank()) {
            gatewayRunByPath(exchange, suffix);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.dashboardRuns());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 202, gateway.createRun(mapper.readValue(exchange.getRequestBody(), java.util.Map.class)));
            return;
        }
        if ("PATCH".equals(exchange.getRequestMethod())) {
            String id = queryString(exchange, "id", "");
            json(exchange, 200, gateway.stopRun(id));
            return;
        }
        if ("DELETE".equals(exchange.getRequestMethod())) {
            String id = queryString(exchange, "id", "");
            json(exchange, gateway.deleteRun(id) ? 200 : 404, Map.of("deleted", id));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void orchestrator(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/orchestrator");
        if (suffix.isBlank() || "surface".equals(suffix)) {
            if (!method(exchange, "GET")) return;
            json(exchange, 200, gateway.orchestratorSurface());
            return;
        }
        if ("inspect".equals(suffix)) {
            if ("GET".equals(exchange.getRequestMethod())) {
                json(exchange, 200, gateway.orchestratorPlan(Map.of(
                        "pipeline", queryString(exchange, "pipeline", "generic-duui-pipeline"),
                        "experiment", queryString(exchange, "experiment", "")
                )));
                return;
            }
            if ("POST".equals(exchange.getRequestMethod())) {
                json(exchange, 200, gateway.orchestratorPlan(mapper.readValue(exchange.getRequestBody(), java.util.Map.class)));
                return;
            }
        }
        methodNotAllowed(exchange);
    }

    private void events(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.events(queryLong(exchange, "limit", 0L)).stream().map(gateway::dashboardEvent).toList());
    }

    private void services(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/gateway/services");
        if (!suffix.isBlank()) {
            serviceByPath(exchange, suffix);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.services());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 201, gateway.putService(mapper.readValue(exchange.getRequestBody(), GatewayServiceDefinition.class)));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void serviceByPath(HttpExchange exchange, String suffix) throws IOException {
        String[] parts = suffix.split("/");
        String id = parts.length > 0 ? parts[0] : "";
        if (id.isBlank()) {
            json(exchange, 400, Map.of("error", "missing service id"));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, 200, gateway.inspectService(id));
            return;
        }
        if ("DELETE".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, gateway.deleteService(id) ? 200 : 404, Map.of("deleted", id));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 2 && "inspect".equals(parts[1])) {
            json(exchange, 200, gateway.inspectService(id));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void storage(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.storageModel());
    }

    private void configurations(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.configurations());
    }

    private void corpora(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpora());
    }

    private void documents(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.documents());
    }

    private void dashboardStatus(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.dashboardStatus());
    }

    private void workbenchPipelines(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.pipelines().stream().map(pipeline -> Map.of(
                "id", pipeline.id(),
                "displayName", pipeline.name(),
                "description", "Gateway-managed DUUI pipeline",
                "annotators", gateway.components().stream().map(component -> component.annotatorId()).toList(),
                "inputModes", java.util.List.of("text", "xmi"),
                "controls", java.util.List.of("stages")
        )).toList());
    }

    private void workbenchAnnotators(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.annotators().stream().map(annotator -> Map.of(
                "id", annotator.id(),
                "displayName", annotator.name(),
                "guidance", annotator.endpoint(),
                "purpose", annotator.endpoint()
        )).toList());
    }

    private void workbenchStages(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.annotators().stream().map(annotator -> Map.of(
                "id", annotator.id(),
                "displayName", annotator.name(),
                "endpoint", annotator.endpoint(),
                "image", annotator.image(),
                "streaming", annotator.tags().stream().anyMatch(tag -> !"legacy".equals(tag)),
                "parameters", java.util.Map.of()
        )).toList());
    }

    private void workbenchRuns(HttpExchange exchange) throws IOException {
        String suffix = pathSuffix(exchange, "/api/runs");
        if (!suffix.isBlank()) {
            runByPath(exchange, suffix);
            return;
        }
        if ("GET".equals(exchange.getRequestMethod())) {
            json(exchange, 200, gateway.dashboardRuns());
            return;
        }
        if ("POST".equals(exchange.getRequestMethod())) {
            json(exchange, 202, gateway.createRun(mapper.readValue(exchange.getRequestBody(), java.util.Map.class)));
            return;
        }
        if ("PATCH".equals(exchange.getRequestMethod())) {
            String id = queryString(exchange, "id", "");
            json(exchange, 200, gateway.stopRun(id));
            return;
        }
        if ("DELETE".equals(exchange.getRequestMethod())) {
            String id = queryString(exchange, "id", "");
            json(exchange, gateway.deleteRun(id) ? 200 : 404, Map.of("deleted", id));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void runByPath(HttpExchange exchange, String suffix) throws IOException {
        String[] parts = suffix.split("/");
        String id = parts.length > 0 ? parts[0] : "";
        if (id.isBlank()) {
            json(exchange, 400, Map.of("error", "missing run id"));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, 200, gateway.dashboardRun(gateway.run(id)));
            return;
        }
        if ("POST".equals(exchange.getRequestMethod()) && parts.length == 2 && "stop".equals(parts[1])) {
            json(exchange, 200, gateway.stopRun(id));
            return;
        }
        if ("DELETE".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, gateway.deleteRun(id) ? 200 : 404, Map.of("deleted", id));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 2 && "events".equals(parts[1])) {
            sseRunEvents(exchange, id);
            return;
        }
        methodNotAllowed(exchange);
    }

    private void sseRunEvents(HttpExchange exchange, String id) throws IOException {
        StringBuilder payload = new StringBuilder();
        for (var event : gateway.runEvents(id, 0)) {
            payload.append("event: ").append(event.type()).append('\n');
            payload.append("data: ").append(mapper.writeValueAsString(gateway.dashboardEvent(event))).append("\n\n");
        }
        payload.append("event: done\ndata: {\"run\":\"").append(id).append("\"}\n\n");
        byte[] body = payload.toString().getBytes(StandardCharsets.UTF_8);
        Headers headers = exchange.getResponseHeaders();
        headers.set("content-type", "text/event-stream; charset=utf-8");
        headers.set("cache-control", "no-cache");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private void emptyList(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpusService().samples());
    }

    private void corpus(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpusService().tree());
    }

    private void corpusArtifact(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpusService().artifact(queryString(exchange, "path", "")));
    }

    private void corpusSelection(HttpExchange exchange) throws IOException {
        if (!method(exchange, "POST")) return;
        Map<?, ?> request = mapper.readValue(exchange.getRequestBody(), Map.class);
        Object paths = request.get("paths");
        List<String> normalized = paths instanceof List<?> list ? list.stream().map(String::valueOf).toList() : List.of();
        json(exchange, 200, gateway.corpusService().selection(normalized));
    }

    private void corpusTypesystem(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpusService().typesystem());
    }

    private void playgroundModel(HttpExchange exchange) throws IOException {
        if (!method(exchange, "GET")) return;
        json(exchange, 200, gateway.corpusService().playgroundModel());
    }

    private void gatewayRunByPath(HttpExchange exchange, String suffix) throws IOException {
        String[] parts = suffix.split("/");
        String id = parts.length > 0 ? parts[0] : "";
        if (id.isBlank()) {
            json(exchange, 400, Map.of("error", "missing run id"));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, 200, gateway.dashboardRun(gateway.run(id)));
            return;
        }
        if ("POST".equals(exchange.getRequestMethod()) && parts.length == 2 && "stop".equals(parts[1])) {
            json(exchange, 200, gateway.dashboardRun(gateway.stopRun(id)));
            return;
        }
        if ("GET".equals(exchange.getRequestMethod()) && parts.length == 2 && "events".equals(parts[1])) {
            json(exchange, 200, gateway.runEvents(id, queryLong(exchange, "limit", 0)).stream().map(gateway::dashboardEvent).toList());
            return;
        }
        if ("DELETE".equals(exchange.getRequestMethod()) && parts.length == 1) {
            json(exchange, gateway.deleteRun(id) ? 200 : 404, Map.of("deleted", id));
            return;
        }
        methodNotAllowed(exchange);
    }

    private void staticAsset(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod()) && !"HEAD".equals(exchange.getRequestMethod())) {
            methodNotAllowed(exchange);
            return;
        }
        Path root = dashboardRoot.toAbsolutePath().normalize();
        if (!Files.exists(root.resolve("index.html")) && root.getParent() != null && Files.exists(root.getParent().resolve("index.html"))) {
            root = root.getParent().toAbsolutePath().normalize();
        }
        String rawPath = exchange.getRequestURI().getPath();
        Path requested = root.resolve(rawPath.equals("/") ? "index.html" : rawPath.substring(1)).normalize();
        if (!requested.startsWith(root) || !Files.exists(requested) || Files.isDirectory(requested)) {
            requested = root.resolve("index.html").normalize();
        }
        if (!Files.exists(requested)) {
            json(exchange, 404, Map.of("error", "dashboard asset not found", "root", root.toString()));
            return;
        }
        byte[] body = Files.readAllBytes(requested);
        Headers headers = exchange.getResponseHeaders();
        headers.set("content-type", contentType(requested));
        headers.set("cache-control", requested.getFileName().toString().equals("index.html") ? "no-cache" : "public, max-age=3600");
        exchange.sendResponseHeaders(200, "HEAD".equals(exchange.getRequestMethod()) ? -1 : body.length);
        if (!"HEAD".equals(exchange.getRequestMethod())) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }

    private boolean method(HttpExchange exchange, String expected) throws IOException {
        if (expected.equals(exchange.getRequestMethod())) {
            return true;
        }
        methodNotAllowed(exchange);
        return false;
    }

    private void methodNotAllowed(HttpExchange exchange) throws IOException {
        json(exchange, 405, Map.of("error", "method not allowed", "method", exchange.getRequestMethod()));
    }

    private void json(HttpExchange exchange, int status, Object value) throws IOException {
        byte[] body = mapper.writeValueAsBytes(value);
        Headers headers = exchange.getResponseHeaders();
        headers.set("content-type", "application/json; charset=utf-8");
        headers.set("x-duui-gateway-time", Instant.now().toString());
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static long queryLong(HttpExchange exchange, String key, long fallback) {
        String value = queryString(exchange, key, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Math.max(0L, Long.parseLong(value));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static String queryString(HttpExchange exchange, String key, String fallback) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null || query.isBlank()) {
            return fallback;
        }
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            if (pair.length == 2 && key.equals(pair[0])) {
                return java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
            }
        }
        return fallback;
    }

    private static String pathSuffix(HttpExchange exchange, String prefix) {
        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(prefix) || path.length() == prefix.length()) {
            return "";
        }
        String suffix = path.substring(prefix.length());
        while (suffix.startsWith("/")) {
            suffix = suffix.substring(1);
        }
        return suffix;
    }

    private static String contentType(Path path) {
        String file = path.getFileName().toString();
        if (file.endsWith(".html")) return "text/html; charset=utf-8";
        if (file.endsWith(".js")) return "text/javascript; charset=utf-8";
        if (file.endsWith(".css")) return "text/css; charset=utf-8";
        if (file.endsWith(".svg")) return "image/svg+xml";
        if (file.endsWith(".png")) return "image/png";
        if (file.endsWith(".jpg") || file.endsWith(".jpeg")) return "image/jpeg";
        return "application/octet-stream";
    }

    @Override
    public void close() {
        server.stop(0);
    }
}

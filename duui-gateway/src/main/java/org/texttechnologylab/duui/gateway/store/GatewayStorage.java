package org.texttechnologylab.duui.gateway.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.texttechnologylab.duui.gateway.model.GatewayAnnotatorRegistration;
import org.texttechnologylab.duui.gateway.model.GatewayComponentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayExperimentDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayPipelineDefinition;
import org.texttechnologylab.duui.gateway.model.GatewayRunSnapshot;
import org.texttechnologylab.duui.gateway.model.GatewayServiceDefinition;
import org.texttechnologylab.duui.storage.DUUIDatabase;
import org.texttechnologylab.duui.storage.DUUICache;
import org.texttechnologylab.duui.storage.DUUIIndex;
import org.texttechnologylab.duui.storage.DUUIInMemoryStorageService;
import org.texttechnologylab.duui.storage.DUUIRegistry;
import org.texttechnologylab.duui.storage.DUUIStorageService;
import org.texttechnologylab.duui.storage.DUUIStoredConfiguration;
import org.texttechnologylab.duui.storage.DUUIStoredCorpus;
import org.texttechnologylab.duui.storage.DUUIStoredDocument;
import org.texttechnologylab.duui.storage.DUUIStoredEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class GatewayStorage {
    private final DUUIStorageService service;
    private final DUUIDatabase<String, GatewayAnnotatorRegistration> annotators;
    private final DUUIDatabase<String, GatewayComponentDefinition> components;
    private final DUUIDatabase<String, GatewayPipelineDefinition> pipelines;
    private final DUUIDatabase<String, GatewayExperimentDefinition> experiments;
    private final DUUIDatabase<String, GatewayServiceDefinition> services;
    private final DUUIDatabase<String, GatewayRunSnapshot> runs;
    private final DUUIDatabase<String, DUUIStoredEvent> events;
    private final DUUIDatabase<String, DUUIStoredConfiguration> configurations;
    private final DUUIDatabase<String, DUUIStoredCorpus> corpora;
    private final DUUIDatabase<String, DUUIStoredDocument> documents;
    private final ObjectMapper mapper;
    private final Path snapshotPath;
    private boolean loading;
    private int bulkDepth;

    public GatewayStorage() {
        this(new DUUIInMemoryStorageService(), null, null);
    }

    public GatewayStorage(ObjectMapper mapper, Path snapshotPath) {
        this(new DUUIInMemoryStorageService(), mapper, snapshotPath);
    }

    public GatewayStorage(DUUIStorageService service) {
        this(service, null, null);
    }

    public GatewayStorage(DUUIStorageService service, ObjectMapper mapper, Path snapshotPath) {
        this.service = service;
        this.mapper = mapper;
        this.snapshotPath = snapshotPath;
        this.annotators = observed(service.namespace("gateway.annotators", GatewayAnnotatorRegistration.class));
        this.components = observed(service.namespace("gateway.components", GatewayComponentDefinition.class));
        this.pipelines = observed(service.namespace("gateway.pipelines", GatewayPipelineDefinition.class));
        this.experiments = observed(service.namespace("gateway.experiments", GatewayExperimentDefinition.class));
        this.services = observed(service.namespace("gateway.services", GatewayServiceDefinition.class));
        this.runs = observed(service.namespace("gateway.runs", GatewayRunSnapshot.class));
        this.events = observed(service.events());
        this.configurations = observed(service.configurations());
        this.corpora = observed(service.corpora());
        this.documents = observed(service.documents());
        loadSnapshot();
    }

    public DUUIStorageService service() {
        return service;
    }

    public DUUIDatabase<String, GatewayAnnotatorRegistration> annotators() {
        return annotators;
    }

    public DUUIDatabase<String, GatewayComponentDefinition> components() {
        return components;
    }

    public DUUIDatabase<String, GatewayPipelineDefinition> pipelines() {
        return pipelines;
    }

    public DUUIDatabase<String, GatewayExperimentDefinition> experiments() {
        return experiments;
    }

    public DUUIDatabase<String, GatewayServiceDefinition> services() {
        return services;
    }

    public DUUIDatabase<String, GatewayRunSnapshot> runs() {
        return runs;
    }

    public DUUIDatabase<String, DUUIStoredEvent> events() {
        return events;
    }

    public DUUIDatabase<String, DUUIStoredConfiguration> configurations() {
        return configurations;
    }

    public DUUIDatabase<String, DUUIStoredCorpus> corpora() {
        return corpora;
    }

    public DUUIDatabase<String, DUUIStoredDocument> documents() {
        return documents;
    }

    public Optional<Path> snapshotPath() {
        return Optional.ofNullable(snapshotPath);
    }

    public synchronized void persist() {
        if (mapper == null || snapshotPath == null || loading || bulkDepth > 0) {
            return;
        }
        try {
            Path parent = snapshotPath.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("annotators", annotators.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("components", components.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("pipelines", pipelines.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("experiments", experiments.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("services", services.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("runs", runs.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("events", events.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("configurations", configurations.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("corpora", corpora.query().list().stream().map(entry -> entry.value()).toList());
            snapshot.put("documents", documents.query().list().stream().map(entry -> entry.value()).toList());
            Path temporary = snapshotPath.resolveSibling(snapshotPath.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), snapshot);
            Files.move(temporary, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not persist DUUI gateway storage snapshot " + snapshotPath, error);
        }
    }

    public void bulkUpdate(Runnable updates) {
        bulkDepth++;
        try {
            updates.run();
        } finally {
            bulkDepth--;
            if (bulkDepth == 0) {
                persist();
            }
        }
    }

    private void loadSnapshot() {
        if (mapper == null || snapshotPath == null || !Files.exists(snapshotPath)) {
            return;
        }
        loading = true;
        try {
            JsonNode root = mapper.readTree(snapshotPath.toFile());
            loadArray(root, "annotators", GatewayAnnotatorRegistration.class, GatewayAnnotatorRegistration::id, annotators);
            loadArray(root, "components", GatewayComponentDefinition.class, GatewayComponentDefinition::id, components);
            loadArray(root, "pipelines", GatewayPipelineDefinition.class, GatewayPipelineDefinition::id, pipelines);
            loadArray(root, "experiments", GatewayExperimentDefinition.class, GatewayExperimentDefinition::id, experiments);
            loadArray(root, "services", GatewayServiceDefinition.class, GatewayServiceDefinition::id, services);
            loadArray(root, "runs", GatewayRunSnapshot.class, GatewayRunSnapshot::id, runs);
            loadArray(root, "events", DUUIStoredEvent.class, DUUIStoredEvent::id, events);
            loadArray(root, "configurations", DUUIStoredConfiguration.class, DUUIStoredConfiguration::id, configurations);
            loadArray(root, "corpora", DUUIStoredCorpus.class, DUUIStoredCorpus::id, corpora);
            loadArray(root, "documents", DUUIStoredDocument.class, DUUIStoredDocument::id, documents);
        } catch (IOException error) {
            throw new UncheckedIOException("Could not load DUUI gateway storage snapshot " + snapshotPath, error);
        } finally {
            loading = false;
        }
    }

    private <R> void loadArray(JsonNode root, String field, Class<R> type, Function<R, String> id, DUUIDatabase<String, R> database) {
        JsonNode array = root.get(field);
        if (array == null || !array.isArray()) {
            return;
        }
        for (JsonNode node : array) {
            R value = mapper.convertValue(node, type);
            database.put(id.apply(value), value);
        }
    }

    private <R> DUUIDatabase<String, R> observed(DUUIDatabase<String, R> delegate) {
        return new ObservedDatabase<>(delegate, this::persist);
    }

    private record ObservedDatabase<R>(DUUIDatabase<String, R> delegate, Runnable onChange) implements DUUIDatabase<String, R> {
        @Override
        public DUUIRegistry.Entry<String, R> put(String key, R record) {
            DUUIRegistry.Entry<String, R> entry = delegate.put(key, record);
            onChange.run();
            return entry;
        }

        @Override
        public Optional<R> get(String key) {
            return delegate.get(key);
        }

        @Override
        public R require(String key) {
            return delegate.require(key);
        }

        @Override
        public Optional<R> delete(String key) {
            Optional<R> removed = delegate.delete(key);
            removed.ifPresent(ignored -> onChange.run());
            return removed;
        }

        @Override
        public Query<String, R> query() {
            return delegate.query();
        }

        @Override
        public DUUIRegistry<String, R> registry() {
            return delegate.registry();
        }

        @Override
        public DUUICache<String, R> cache() {
            return delegate.cache();
        }

        @Override
        public <I> DUUIIndex<I, String> index(String name, Class<I> keyType) {
            return delegate.index(name, keyType);
        }

        @Override
        public <I> DUUIIndex<I, String> index(String name, Class<I> keyType, Function<R, I> extractor) {
            return delegate.index(name, keyType, extractor);
        }
    }
}

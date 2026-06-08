package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.orchestration.worker.DUUIExecutor;
import org.texttechnologylab.duui.timelines.DUUIDispatcher;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton container for orchestrator-global resources.
 * Holds DUUIExecutor, DUUIPool registry, configuration, and telemetry.
 * Bidirectional config bus: orchestrator pushes config, components pull config.
 *
 * <p>Access: {@code DUUIRuntime.getInstance(orchestratorId)}</p>
 *
 * [DESIGN: lines 168-198]
 */
public final class DUUIRuntime {

    private static final ConcurrentHashMap<String, DUUIRuntime> INSTANCES = new ConcurrentHashMap<>();

    private final String orchestratorId;
    private final DUUIExecutor executor;
    private final Map<String, DUUIPool<?>> pools = new ConcurrentHashMap<>();
    private final Map<Class<?>, Object> configurations = new ConcurrentHashMap<>();

    private DUUIRuntime(String orchestratorId, DUUIExecutor executor) {
        this.orchestratorId = Objects.requireNonNull(orchestratorId, "orchestratorId");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public static DUUIRuntime getInstance(String orchestratorId) {
        return INSTANCES.computeIfAbsent(orchestratorId,
                id -> new DUUIRuntime(id, DUUIExecutor.getInstance(id)));
    }

    public static DUUIRuntime getInstance(String orchestratorId, DUUIExecutor executor) {
        return INSTANCES.computeIfAbsent(orchestratorId,
                id -> new DUUIRuntime(id, executor));
    }

    /**
     * Register a named pool in the runtime registry.
     *
     * @param name descriptive name for identification [DESIGN: line 279]
     * @param pool the DUUIPool instance
     * @param <T>  pool element type
     */
    public <T> void registerPool(String name, DUUIPool<T> pool) {
        pools.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(pool, "pool"));
    }

    /**
     * Retrieve a registered pool by name.
     *
     * @param name pool identifier
     * @param <T>  pool element type
     * @return the registered pool, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> DUUIPool<T> pool(String name) {
        return (DUUIPool<T>) pools.get(name);
    }

    /**
     * Push configuration from orchestrator into the runtime.
     * Components can pull this configuration at any time.
     *
     * @param type   configuration type key
     * @param config configuration object
     * @param <T>    configuration type
     */
    public <T> void pushConfig(Class<T> type, T config) {
        if (config == null) {
            configurations.remove(type);
        } else {
            configurations.put(type, config);
        }
    }

    /**
     * Pull configuration from the runtime.
     * Used by components to read orchestrator-provided settings.
     *
     * @param type configuration type key
     * @param <T>  configuration type
     * @return the configuration, or null if not set
     */
    @SuppressWarnings("unchecked")
    public <T> T pullConfig(Class<T> type) {
        return (T) configurations.get(type);
    }

    public DUUIExecutor executor() {
        return executor;
    }

    public String orchestratorId() {
        return orchestratorId;
    }

    /**
     * Close and remove the runtime, shutting down the executor.
     */
    public void close() {
        INSTANCES.remove(orchestratorId);
        executor.close();
        pools.clear();
        configurations.clear();
    }
}

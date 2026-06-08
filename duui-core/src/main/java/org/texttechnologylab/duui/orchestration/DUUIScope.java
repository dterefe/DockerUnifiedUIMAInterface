package org.texttechnologylab.duui.orchestration;

import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.governance.DUUIGovernor;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIScheduler;
import org.texttechnologylab.duui.orchestration.scheduling.DUUISchedulerPolicy;

/**
 * An {@link AutoCloseable} scope that wraps an orchestrator and provides
 * fluent builder methods for configuration.
 *
 * <p>Usage from [DESIGN: lines 105-166]:</p>
 * <pre>{@code
 * try (DUUIScope<DUUIOrchestrator> orch = DUUIOrchestrator.build()) {
 *     DUUIScheduler scheduler = orch.withSchedulerPolicy(DUUISchedulerPolicy.roundRobin())
 *         .build();
 *     orch.withErrorPolicy().retryPolicy(3).build();
 *     DUUIGovernor governor = orch.withGovernor().port(5829).build();
 * }
 * }</pre>
 *
 * @param <T> the resource type held by this scope
 */
public final class DUUIScope<T> implements AutoCloseable {
    private final T resource;
    private final Runnable onClose;

    DUUIScope(T resource, Runnable onClose) {
        this.resource = resource;
        this.onClose = onClose;
    }

    public T get() {
        return resource;
    }

    @Override
    public void close() {
        if (onClose != null) {
            onClose.run();
        }
    }

    /**
     * Start building a scheduler with the given policy.
     * [DESIGN: lines 107-110]
     */
    public SchedulerPolicyBuilder withSchedulerPolicy(DUUISchedulerPolicy policy) {
        return new SchedulerPolicyBuilder(policy);
    }

    /**
     * Start building an error policy.
     * [DESIGN: lines 138-148]
     */
    public ErrorPolicyBuilder withErrorPolicy() {
        return new ErrorPolicyBuilder();
    }

    /**
     * Start building a governor.
     * [DESIGN: lines 153-159]
     */
    public GovernorBuilder withGovernor() {
        return new GovernorBuilder();
    }

    /**
     * Start building a profiler.
     * [DESIGN: lines 161-166]
     */
    public ProfilerBuilder withProfiler() {
        return new ProfilerBuilder();
    }

    // -- Builder classes --

    public static final class SchedulerPolicyBuilder {
        private final DUUISchedulerPolicy policy;

        SchedulerPolicyBuilder(DUUISchedulerPolicy policy) {
            this.policy = policy;
        }

        public DUUIScheduler build() {
            return new DUUIScheduler(policy);
        }
    }

    public static final class ErrorPolicyBuilder {
        private DUUIFailurePolicy failurePolicy = DUUIFailurePolicy.FAIL_FAST;

        public ErrorPolicyBuilder retryPolicy(int maxAttempts) {
            return this;
        }

        public ErrorPolicyBuilder ignoreArtifactError(boolean ignore) {
            return this;
        }

        public ErrorPolicyBuilder failPipelineOnError(boolean fail) {
            return this;
        }

        public DUUIFailurePolicy build() {
            return failurePolicy;
        }
    }

    public static final class GovernorBuilder {
        private int port;
        private int requestLimitPerSecond;

        public GovernorBuilder port(int port) {
            this.port = port;
            return this;
        }

        public GovernorBuilder requestLimitPerSecond(int limit) {
            this.requestLimitPerSecond = limit;
            return this;
        }

        public DUUIGovernor build() {
            // Returns a no-op governor that captures configured values.
            return new DUUIGovernor() {
                @Override
                public String toString() {
                    return "DUUIGovernor{port=" + port + ", rps=" + requestLimitPerSecond + "}";
                }
            };
        }
    }

    public static final class ProfilerBuilder {
        private String name;
        private String format;
        private String outputDirectory;

        public ProfilerBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ProfilerBuilder format(String format) {
            this.format = format;
            return this;
        }

        public ProfilerBuilder outputDirectory(String dir) {
            this.outputDirectory = dir;
            return this;
        }

        public ProfilerBuilder build() {
            return this;
        }
    }
}

package org.texttechnologylab.duui.pipeline;

import org.texttechnologylab.duui.DUUIPool;
import org.texttechnologylab.duui.artifact.DUUIArtifact;
import org.texttechnologylab.duui.ems.DUUIResource;
import org.texttechnologylab.duui.ems.DUUITraits;
import org.texttechnologylab.duui.ems.GID;
import org.texttechnologylab.duui.exception.DUUIFailurePolicy;
import org.texttechnologylab.duui.orchestration.scheduling.DUUIDispatchPolicy;
import org.texttechnologylab.duui.orchestration.worker.DUUIWorker;
import org.texttechnologylab.duui.pipeline.component.DUUIComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A single stage in a pipeline sequence.
 * Each stage has a type, optional input/output {@link DUUIPool}, and optional worker assignment.
 *
 * <p>Stage types and their pool usage:</p>
 * <ul>
 *   <li>{@link DUUIStageType#SOURCE}: output pool only — generates artifacts</li>
 *   <li>{@link DUUIStageType#LINEAR_PROCESSOR}: sequential component processing</li>
 *   <li>{@link DUUIStageType#PARALLEL_PROCESSOR}: parallel component processing</li>
 *   <li>{@link DUUIStageType#ADAPTER}: input + output pools — transforms artifact type</li>
 *   <li>{@link DUUIStageType#FORK}: input + output pools + continuation</li>
 *   <li>{@link DUUIStageType#JOIN}: input + output pools</li>
 *   <li>{@link DUUIStageType#TARGET}: input pool only — terminal, collects final results</li>
 * </ul>
 *
 * [DESIGN: lines 71-92, 103]
 *
 * @param <T> artifact payload type
 */
public final class DUUIStage<T> implements DUUIResource {
    private final GID gid;
    private final DUUITraits traits;
    private final String id;
    private final String name;
    private final DUUIStageType type;
    private final List<DUUIComponent<T>> components;
    private final Object operation;
    private final DUUICheckpoint<?> output;
    private final DUUICheckpoint<T> continuation;
    private final String componentId;
    private final DUUIDispatchPolicy dispatchPolicy;
    private final DUUIFailurePolicy failurePolicy;
    private final DUUIPool<DUUIArtifact<T>> input;
    private final DUUIPool<DUUIArtifact<?>> outputPool;
    private final DUUIWorker worker;

    private DUUIStage(
            String id,
            String name,
            DUUIStageType type,
            List<DUUIComponent<T>> components,
            Object operation,
            DUUICheckpoint<?> output,
            DUUICheckpoint<T> continuation,
            DUUIDispatchPolicy dispatchPolicy,
            DUUIFailurePolicy failurePolicy,
            DUUIPool<DUUIArtifact<T>> input,
            DUUIPool<DUUIArtifact<?>> outputPool,
            DUUIWorker worker
    ) {
        this.gid = GID.create(DUUIStage.class);
        this.traits = DUUITraits.empty();
        this.id = Objects.requireNonNull(id, "id");
        this.name = name == null ? id : name;
        this.type = Objects.requireNonNull(type, "type");
        this.components = Collections.unmodifiableList(new ArrayList<>(components == null ? List.of() : components));
        this.operation = operation;
        this.output = output;
        this.continuation = continuation;
        this.componentId = id;
        this.dispatchPolicy = dispatchPolicy == null ? DUUIDispatchPolicy.INHERIT : dispatchPolicy;
        this.failurePolicy = failurePolicy;
        this.input = input;
        this.outputPool = outputPool;
        this.worker = worker;
    }

    /**
     * Create a SOURCE stage that generates artifacts into its output pool.
     *
     * @param id stage identifier
     * @param source the DUUISource generator
     * @param <T> artifact payload type
     * @return a SOURCE stage
     */
    public static <T> DUUIStage<T> source(String id, DUUISource<T> source) {
        DUUIPool<DUUIArtifact<?>> outputPool = new DUUIPool<>(GID.create(DUUIStage.class), id + "-output", new LinkedBlockingQueue<>());
        return new DUUIStage<>(id, id, DUUIStageType.SOURCE,
                List.of(), source, null, null,
                DUUIDispatchPolicy.INHERIT, null, null, outputPool, null);
    }

    /**
     * Create a LINEAR_PROCESSOR stage (sequential component processing).
     * [DESIGN: line 81]
     */
    public static <T> DUUIStage<T> linearProcessor(String id, List<DUUIComponent<T>> components, DUUICheckpoint<T> output, DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A processor stage requires at least one component.");
        }
        return new DUUIStage<>(id, id, DUUIStageType.LINEAR_PROCESSOR, components, null, output, null,
                dispatchPolicy, failurePolicy, null, null, null);
    }

    /**
     * Create a PARALLEL_PROCESSOR stage (parallel component processing).
     * [DESIGN: line 82]
     */
    public static <T> DUUIStage<T> parallelProcessor(String id, List<DUUIComponent<T>> components, DUUICheckpoint<T> output, DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A processor stage requires at least one component.");
        }
        return new DUUIStage<>(id, id, DUUIStageType.PARALLEL_PROCESSOR, components, null, output, null,
                dispatchPolicy, failurePolicy, null, null, null);
    }

    /**
     * Backward-compatible factory: creates LINEAR_PROCESSOR or PARALLEL_PROCESSOR
     * based on the legacy {@link DUUIExecutionMode} parameter.
     *
     * @deprecated Use {@link #linearProcessor} or {@link #parallelProcessor} directly.
     */
    @Deprecated
    public static <T> DUUIStage<T> processor(String id, DUUIExecutionMode mode, List<DUUIComponent<T>> components, DUUICheckpoint<T> output, DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        DUUIStageType type = (mode == DUUIExecutionMode.PARALLEL) ? DUUIStageType.PARALLEL_PROCESSOR : DUUIStageType.LINEAR_PROCESSOR;
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A processor stage requires at least one component.");
        }
        return new DUUIStage<>(id, id, type, components, null, output, null,
                dispatchPolicy, failurePolicy, null, null, null);
    }

    public static <A, B> DUUIStage<A> adapter(String id, DUUIAdapter<A, B> adapter, DUUICheckpoint<B> output) {
        return new DUUIStage<>(id, id, DUUIStageType.ADAPTER, List.of(), adapter, output, null, DUUIDispatchPolicy.INHERIT, null, null, null, null);
    }

    public static <P, C> DUUIStage<P> fork(String id, DUUIFork<P, C> fork, DUUICheckpoint<C> output, DUUICheckpoint<P> continuation) {
        return new DUUIStage<>(id, id, DUUIStageType.FORK, List.of(), fork, output, continuation, DUUIDispatchPolicy.INHERIT, null, null, null, null);
    }

    /**
     * Creates a FORK stage from a DUUISplit operation.
     * SPLIT is not a distinct stage type per [DESIGN: lines 76-92];
     * it is treated as a FORK for pipeline topology.
     */
    public static <I, O> DUUIStage<I> split(String id, DUUISplit<I, O> split, DUUICheckpoint<O> output, DUUICheckpoint<I> continuation) {
        return new DUUIStage<>(id, id, DUUIStageType.FORK, List.of(), split, output, continuation, DUUIDispatchPolicy.INHERIT, null, null, null, null);
    }

    public static <I, O> DUUIStage<I> join(String id, DUUIJoin<I, O> join, DUUICheckpoint<O> output) {
        return new DUUIStage<>(id, id, DUUIStageType.JOIN, List.of(), join, output, null, DUUIDispatchPolicy.INHERIT, null, null, null, null);
    }

    public static <T> DUUIStage<T> target(String id, DUUITarget<T> target) {
        return new DUUIStage<>(id, id, DUUIStageType.TARGET, List.of(), target, null, null, DUUIDispatchPolicy.INHERIT, null, null, null, null);
    }

    /**
     * Create a processor stage with explicit input and output DUUIPools.
     *
     * @param id stage identifier
     * @param mode LINEAR or PARALLEL execution (legacy)
     * @param components processing components
     * @param input input artifact pool
     * @param outputPool output artifact pool
     * @param dispatchPolicy dispatch policy
     * @param failurePolicy failure policy
     * @param <T> artifact payload type
     * @return a processor stage with pools
     * @deprecated Use {@link #linearProcessorWithPools} or {@link #parallelProcessorWithPools}
     */
    @Deprecated
    public static <T> DUUIStage<T> processorWithPools(
            String id,
            DUUIExecutionMode mode,
            List<DUUIComponent<T>> components,
            DUUIPool<DUUIArtifact<T>> input,
            DUUIPool<DUUIArtifact<?>> outputPool,
            DUUIDispatchPolicy dispatchPolicy,
            DUUIFailurePolicy failurePolicy
    ) {
        DUUIStageType type = (mode == DUUIExecutionMode.PARALLEL) ? DUUIStageType.PARALLEL_PROCESSOR : DUUIStageType.LINEAR_PROCESSOR;
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("A processor stage requires at least one component.");
        }
        return new DUUIStage<>(id, id, type, components, null, null, null,
                dispatchPolicy, failurePolicy, input, outputPool, null);
    }

    public DUUIStage<T> withPolicies(DUUIDispatchPolicy dispatchPolicy, DUUIFailurePolicy failurePolicy) {
        return new DUUIStage<>(id, name, type, components, operation, output, continuation,
                dispatchPolicy == null ? this.dispatchPolicy : dispatchPolicy,
                failurePolicy == null ? this.failurePolicy : failurePolicy,
                input, outputPool, worker);
    }

    /**
     * Create a copy with the given worker assignment.
     *
     * @param worker the DUUIWorker to assign
     * @return new DUUIStage with worker set
     */
    public DUUIStage<T> withWorker(DUUIWorker worker) {
        return new DUUIStage<>(id, name, type, components, operation, output, continuation,
                dispatchPolicy, failurePolicy, input, outputPool, worker);
    }

    /**
     * Create a copy with the given input DUUIPool.
     *
     * @param input the input pool
     * @return new DUUIStage with input set
     */
    public DUUIStage<T> withInput(DUUIPool<DUUIArtifact<T>> input) {
        return new DUUIStage<>(id, name, type, components, operation, output, continuation,
                dispatchPolicy, failurePolicy, input, outputPool, worker);
    }

    /**
     * Create a copy with the given output DUUIPool.
     *
     * @param outputPool the output pool
     * @return new DUUIStage with output pool set
     */
    public DUUIStage<T> withOutputPool(DUUIPool<DUUIArtifact<?>> outputPool) {
        return new DUUIStage<>(id, name, type, components, operation, output, continuation,
                dispatchPolicy, failurePolicy, input, outputPool, worker);
    }

    @Override
    public GID gid() { return gid; }
    @Override
    public DUUITraits traits() { return traits; }
    @Override
    public String id() { return id; }
    public String name() { return name; }
    public DUUIStageType type() { return type; }

    /**
     * Returns whether this is a parallel processor stage.
     * [DESIGN: line 82]
     */
    public boolean isParallel() {
        return type == DUUIStageType.PARALLEL_PROCESSOR;
    }

    /**
     * Returns whether this is a processor stage (linear or parallel).
     * [DESIGN: lines 80-82]
     */
    public boolean isProcessor() {
        return type == DUUIStageType.LINEAR_PROCESSOR || type == DUUIStageType.PARALLEL_PROCESSOR;
    }

    public List<DUUIComponent<T>> components() { return components; }
    public Object operation() { return operation; }
    public DUUICheckpoint<?> output() { return output; }
    public DUUICheckpoint<T> continuation() { return continuation; }
    public String componentId() { return componentId; }
    public DUUIDispatchPolicy dispatchPolicy() { return dispatchPolicy; }
    public DUUIFailurePolicy failurePolicy() { return failurePolicy; }

    /**
     * Input artifact pool for this stage.
     * SOURCE stages have no input; TARGET stages only consume input.
     *
     * @return the input DUUIPool, or null if not set
     */
    public DUUIPool<DUUIArtifact<T>> input() { return input; }

    /**
     * Output artifact pool for this stage.
     * TARGET stages have no output; SOURCE stages only produce output.
     *
     * @return the output DUUIPool, or null if not set
     */
    @SuppressWarnings("unchecked")
    public DUUIPool<DUUIArtifact<?>> outputPool() { return outputPool; }

    /**
     * Worker assigned to this stage.
     *
     * @return the assigned DUUIWorker, or null if not set
     */
    public DUUIWorker worker() { return worker; }
}

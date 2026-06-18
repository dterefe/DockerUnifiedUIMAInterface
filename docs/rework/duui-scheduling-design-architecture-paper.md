# DUUI Scheduling Design Architecture: An End-to-End Research Description

## Abstract

The DUUI scheduling architecture is a checkpoint-centered orchestration design for UIMA-based, containerized, and remote annotation pipelines. Its core problem is not merely to run tasks concurrently, but to preserve a coherent relationship between corpus artifacts, stage-local admission, worker capacity, execution lanes, observability, and the operational limits of distributed annotator services. The current design decomposes scheduling into a small, auditable decision function and a set of supporting resource carriers: stages define the processing topology, checkpoints buffer admissible work, queued tasks preserve priority and sequence, and the pipeline executor materializes work on platform or virtual-thread lanes. This paper documents the architecture end to end, relates it to queueing theory and site reliability practice, and explains how DUUI's implementation turns abstract scheduling concerns into concrete interfaces in `DUUIScheduler`, `DUUIPipeline`, `DUUICheckpoint`, `DUUITaskQueue`, and the telemetry metric catalog.

## 1. Introduction

DUUI exists to make heterogeneous analysis components usable as a single processing pipeline. In a typical BioFID-shaped workflow, an artifact may pass through tokenization, geocoding, taxonomic entity recognition, and target serialization while individual annotators are provided by local UIMA code, remote HTTP endpoints, Docker or Podman containers, Swarm services, or Kubernetes deployments. The scheduler therefore sits at a structural boundary. It cannot assume that all work is CPU-bound, equally expensive, colocated, or even served by the same runtime. At the same time, it must remain narrow enough to be testable and understandable.

The present DUUI scheduling design is deliberately not a monolithic global optimizer. It is an admission and selection layer over pipeline checkpoints. It chooses the next ready task from partitioned checkpoint queues, records why it made that choice, and assigns the selected work to an execution lane. Concurrency, component capacity, container lifecycle, and remote endpoint behavior are owned by adjacent subsystems. This separation is important because DUUI's workload is a mixed queueing system: document arrivals may be bursty; service times differ by annotator, document length, model type, and network path; and failures may be semantic, transient, or infrastructural. A scheduler that tried to internalize all of this would quickly become opaque. DUUI instead exposes a policy seam through `DUUISchedulerPolicy` while shipping a conservative default policy that can be reasoned about from first principles.

Queueing theory gives the architecture its simplest mental model. Little's law states that, under regularity assumptions, the long-run number of items in a system equals arrival rate multiplied by mean time in the system. Little's 1961 proof formulates this as `L = lambda W` for stationary queueing processes with finite means and a nonzero arrival process.[^little] In DUUI terms, checkpoint depth is the local `L`, enqueue/dequeue rates approximate flow, and task/checkpoint wait metrics approximate `W`. The formula does not by itself prescribe a scheduling algorithm, but it explains why queue depth and wait time are first-class scheduler evidence rather than incidental counters.

Kingman's formula complements that view by showing why high utilization is dangerous for variable workloads. The G/G/1 approximation relates mean queue waiting time to utilization, arrival variability, service variability, and mean service time; as utilization approaches one, the utilization term grows sharply.[^kingman] DUUI's scheduling objectives therefore include not only throughput and progress but also utilization, saturation, and pressure constraints. The goal is not to run every lane at maximum occupancy, but to keep work moving while avoiding the unstable region where queues grow faster than annotators can drain them.

The architectural consequence is that DUUI treats scheduling as a control surface, not simply as a dispatch convenience. A conventional executor queue asks only which runnable should be taken next. DUUI asks a wider question: which stage-local artifact task is admissible, ready, explainable, and safe to advance under the current worker and downstream conditions? That question spans code structure, runtime deployment, and scientific reproducibility. If a batch corpus run produces different annotations because tasks were reordered around hidden failures, the scheduler has affected research validity. If a model-serving endpoint is overloaded because the pipeline treats every remote wait as cheap parallelism, the scheduler has affected operational reliability. If queue depth grows but no metric distinguishes source pressure from downstream errors, the scheduler has failed to produce useful evidence. The current design therefore establishes a small but complete scheduling vocabulary before attempting more aggressive optimization.

## 2. Related Research and Design Lineage

DUUI's scheduling architecture sits at the intersection of queueing theory, data-parallel processing, cluster scheduling, multi-resource fairness, randomized load balancing, and production reliability engineering. It is not a direct implementation of any single paper. Instead, it borrows the strongest architectural lesson from each area: expose the queue, measure the wait, keep scheduling policy replaceable, and separate semantic pipeline ordering from lower-level resource placement.

The queueing lineage begins with Little's law and classical queueing systems. Little's proof is useful because it is intentionally general: it does not require a particular service discipline such as FIFO, only stable long-run averages under the stated assumptions.[^little] Kleinrock's queueing systems work gives the broader mathematical frame for reasoning about waiting, service, and resource contention in computer systems.[^kleinrock] DUUI's checkpoint depth, enqueue/dequeue rates, and wait metrics follow from this lineage. They provide the raw observables needed before a scheduler can claim that a stage is stable, saturated, or underutilized.

Kingman's heavy-traffic approximation extends the argument from average flow balance to the operational danger of variability.[^kingman] DUUI annotator stages are variable by construction: document length, OCR quality, model complexity, network latency, serialization cost, and storage backend behavior all affect service time. This makes mean-only reasoning misleading. Dean and Barroso's "Tail at Scale" gives the distributed-systems version of the same warning: in large fan-out systems, even rare slow responses can dominate user-visible latency.[^tail] DUUI pipelines may not be web search serving paths, but they share the same composition problem. A document that passes through several heterogeneous annotators inherits the tail behavior of each stage.

Data-parallel systems research contributes a second lineage. MapReduce made large-scale data processing accessible by separating user computation from distributed execution, fault handling, and data movement.[^mapreduce] DUUI is not MapReduce, but it shares the architectural move of hiding heterogeneous execution behind a higher-level processing model. Where MapReduce uses map and reduce functions over key-value pairs, DUUI uses UIMA artifacts, stages, and annotator components. The scheduling issue is correspondingly more semantic: DUUI cannot treat every task as an independent data-parallel function because annotation stages have ordered CAS transformations, type-system effects, and component-specific runtime constraints.

Cluster scheduling research contributes the third lineage. Mesos introduced a thin resource-sharing layer with two-level scheduling, giving frameworks a common way to access cluster resources while preserving framework-level decisions.[^mesos] Omega explored shared-state scheduling with optimistic concurrency to avoid the limits of monolithic and two-level approaches at Google scale.[^omega] Borg demonstrated production cluster management over hundreds of thousands of jobs and many application types.[^borg] Borg, Omega, and Kubernetes together show that resource scheduling is layered: cluster managers place processes and containers; application frameworks still need their own semantic scheduling.[^bok] DUUI's scheduler belongs in this upper semantic layer. It does not replace Kubernetes, Podman, Docker, Swarm, or a remote service's internal queue. It decides which DUUI artifact task should advance next.

Multi-resource fairness research is relevant because DUUI tasks consume more than one resource type. Dominant Resource Fairness generalizes max-min fairness to settings where users have heterogeneous demands over multiple resources.[^drf] DUUI does not currently implement DRF, but the idea clarifies why a single "workers" count is insufficient. A stage can be CPU-heavy, GPU-heavy, memory-heavy, network-heavy, storage-heavy, or remote-endpoint-heavy. A future DUUI policy that allocates progress across corpora, annotator families, or tenants should consider dominant shares rather than only task counts.

Low-latency distributed scheduling research provides another contrast. Sparrow shows that decentralized randomized sampling and late binding can achieve low-latency scheduling for short tasks without a centralized scheduler becoming a bottleneck.[^sparrow] The power-of-two-choices literature similarly shows that a small amount of random choice can dramatically improve load balance compared with pure random placement.[^twochoices] DUUI's current scheduler is centralized inside one pipeline executor, so it does not need Sparrow's decentralized architecture yet. The research is still relevant because it suggests future paths if DUUI runs many independent scheduler agents or many remote worker pools.

Production reliability literature contributes the final lineage. Google SRE's monitoring guidance foregrounds latency, traffic, errors, and saturation as the four golden signals.[^sre-monitoring] The SLO chapter emphasizes explicit performance expectations rather than implicit operator assumptions.[^sre-slo] The overload chapter states the production reality directly: even good load-balancing policies eventually encounter overload, and reliable systems must handle overload gracefully.[^sre-overload] DUUI's checkpoint modes, backpressure counters, downstream latency indicators, and pressure objectives are aligned with this reliability view. They make overload a schedulable condition, not a surprise after the executor queue has already grown without bound.

## 3. Design Goals

The scheduling design serves seven architectural goals.

First, scheduling must be artifact-aware. DUUI pipelines process typed artifacts, not anonymous jobs. A scheduled unit carries a task, a stage, an artifact, component attributes, resource specifications, observability specifications, and globally unique entity identifiers.

Second, scheduling must be stage-partitioned. Work is admitted through checkpoints owned by stages. This avoids a single undifferentiated queue and allows the system to reason about backlog, readiness, and downstream health per checkpoint.

Third, scheduling must remain dependency-aware. The default policy filters out tasks whose dependencies are not ready. The checkpoint itself also skips dependency-blocked worker tasks when polling. This prevents the executor from consuming work that cannot make progress.

Fourth, scheduling must preserve predictable ordering. `DUUIQueuedTask` implements priority-first, sequence-second ordering. Higher priority values are selected before lower priority values, and equal-priority work follows insertion sequence. The default scheduler selects the minimum natural ordering across checkpoint snapshots, yielding a stable priority discipline across partitions.

Fifth, scheduling must separate selection from execution. `DUUISchedulingDecision` says whether to advance or wait, which checkpoint and task were selected, which lane should run it, and which attributes explain the decision. Actual execution occurs in `DUUIPipeline` through platform threads or a virtual-thread executor.

Sixth, scheduling must be observable. Decisions, checkpoint depth, checkpoint wait, task queue wait, execution duration, active workers, inflight tasks, and backpressure are cataloged as measurable surfaces. Google SRE practice frames monitoring as the collection, processing, aggregation, and display of quantitative system data, and recommends dashboards that cover latency, traffic, errors, and saturation.[^sre-monitoring] DUUI maps those operational categories onto pipeline concepts.

Seventh, scheduling must allow policy replacement without turning every caller into a scheduler author. The concrete `DUUIScheduler` is final in behavior shape but delegates policy to `DUUISchedulerPolicy`. The default is checkpoint priority selection; later policies can use the same request and decision records to implement pressure-sensitive, SLO-aware, or workload-specific strategies.

These goals produce a deliberately layered system. The scheduler is allowed to know about queues, readiness, objectives, capabilities, and explainable decisions. It is not allowed to become the owner of every downstream mechanism that might affect queue state. This is a practical line: once a scheduler directly owns container startup, HTTP retry, UIMA type-system merging, storage write policy, and GPU placement, it becomes impossible to test a scheduling decision without replaying the world. DUUI instead keeps those mechanisms observable and addressable, while the scheduler consumes a normalized request and emits a normalized decision.

## 4. Architectural Overview

The scheduler is centered on five runtime objects:

1. `DUUIPipeline` owns the executor, stages, component initialization, source iteration, and task submission.
2. `DUUIStage` names a source, processor, or target stage and owns its checkpoint.
3. `DUUICheckpoint<T>` admits, buffers, snapshots, and removes work.
4. `DUUIScheduler` turns checkpoint snapshots plus capabilities, objectives, and workload attributes into a scheduling decision.
5. `DUUIQueuedTask` wraps a `DUUITask` as executor-visible work with priority, sequence, partition, lane, and stage metadata.

The data path begins when pipeline code creates a `DUUITask` for a processing stage. The task is marked queued, inherits the current worker context when present, and is wrapped as a `DUUIQueuedTask`. Submission to the pipeline executor does not place it into a plain Java queue. Instead, the executor uses `DUUIMultiplexedTaskQueue`, a custom `BlockingQueue<Runnable>` that accepts only `DUUIQueuedTask` instances. The queue partitions tasks by checkpoint identifier. On offer, it locates or creates the partition checkpoint, calls `checkpoint.accept(queuedTask)`, increments the total queue size, and signals waiting worker threads.

When an executor thread asks for work, `DUUIMultiplexedTaskQueue` constructs a scheduler request from all checkpoint partitions and calls `DUUIScheduler.schedule(...)`. The default policy flattens checkpoint snapshots, filters for `task.dependenciesReady()`, orders queued tasks by priority and sequence, and returns either an `ADVANCE` decision or a `WAIT` decision. If work advances, the queue applies the selected execution lane, removes the selected task from its checkpoint, drops empty partitions, decrements total size, and hands the runnable to the executor.

The runnable then executes through `DUUIQueuedTask.run()`. Platform-lane work runs directly on the `ThreadPoolExecutor` worker. Virtual-lane work is delegated to the pipeline's virtual-thread executor and then run by a `DUUIWorker` with kind `VIRTUAL`. In both cases, the actual callable is run through worker context machinery, so scheduling is only the handoff point between admitted work and worker execution.

This produces a layered architecture:

```text
Source or component code
  -> DUUITask
  -> DUUIQueuedTask
  -> DUUIMultiplexedTaskQueue
  -> DUUICheckpoint partition
  -> DUUISchedulerPolicy decision
  -> DUUIExecutionLane
  -> DUUIWorker
  -> Component process / artifact result
```

The design is intentionally modest. The scheduler does not start containers, borrow annotator replicas, serialize CAS payloads, retry failed HTTP calls, or decide Kubernetes placement. Those concerns belong to drivers, components, workers, and resource clients. Scheduling observes their pressure through attributes and metrics, and future policies can act on that evidence without coupling the selection algorithm to every runtime implementation.

### 4.1 Lifecycle of a Scheduled Artifact

A scheduled artifact passes through a sequence of states that are distributed across several DUUI concepts. At the pipeline boundary, the source emits or materializes an artifact. In the current pipeline implementation, source processing begins with an empty artifact admitted through the source checkpoint and transformed into a `JCas` artifact by the source task. For every processor stage, the pipeline constructs a `DUUITask` whose callable invokes the corresponding component on the current artifact. That task is not yet scheduled in the executor sense; it is first a trackable unit with a gid, actor, artifact, callable, attributes, and lifecycle timestamps.

When the task is submitted, `enqueueStageTask(...)` marks it as queued and wraps it as `DUUIQueuedTask`. This is the point where scheduling metadata becomes explicit: the queued task records its stage, requested lane, priority, sequence number, and executor support for virtual threads. The sequence number is global across queued tasks, so equal-priority work has deterministic age ordering. The stage supplies the checkpoint, and the checkpoint supplies the partition key.

The custom queue then accepts the queued task into its checkpoint partition. This matters because checkpoint admission and executor queueing are not two unrelated mechanisms. The queue cannot contain work that the checkpoint has rejected. Once the task is accepted, it is visible both as executor backlog and as checkpoint depth. A subsequent scheduler decision therefore operates over the same admitted work that the executor will run. There is no shadow queue whose contents diverge from the checkpoint snapshot.

After selection, the task moves into worker execution. `beforeExecute(...)` marks the task as dispatched for platform-thread execution, while virtual-lane execution also marks dispatch before handing off to the virtual executor. The worker then runs the task and records active, completion, cancellation, or failure state through the task lifecycle. The result completes the task's future, and the pipeline uses that result as the input artifact for the next component stage. The scheduler's decision is therefore one step in a longer artifact transformation chain, but it is the step that turns admitted, ready work into actual worker consumption.

### 4.2 Why the Queue Is Multiplexed

A single FIFO queue would be simpler, but it would hide the structure DUUI needs to operate distributed pipelines. Stage-local backlog is meaningful. A queue of 10,000 tasks is not enough information if 9,900 are waiting for a slow taxonomic model and 100 are waiting for a fast serializer. Checkpoint partitioning preserves the stage identity of pressure. It also allows policy code to observe several queues at once without embedding stage-specific logic in the executor.

The multiplexed queue also keeps the scheduler in the hot path where it matters. Selection happens when executor workers request work, not only when producers submit work. That means the decision can account for the current partition set and current readiness. If a dependency becomes ready after submission, the task can be selected on the next poll. If a checkpoint drains and disappears from the partition map, it no longer participates in selection. This is a lightweight form of dynamic scheduling without a persistent scheduler thread or complex event loop.

The tradeoff is that selection scans checkpoint snapshots. For the current architecture this is acceptable because correctness, explainability, and partition preservation are higher priorities than a specialized heap. If future workloads need very large numbers of queued tasks per checkpoint, the same public policy contract can be retained while the queue internals evolve toward indexed ready sets, per-checkpoint priority heaps, or staged sampling.

## 5. Checkpoints as Scheduling Boundaries

`DUUICheckpoint` is the central abstraction for admission and queue state. A checkpoint has a mode, an admission predicate, a bounded or unbounded `DUUITaskQueue`, counters for accepted, rejected, cancelled, enqueued, and dequeued items, and downstream indicators for worker availability, phase latency, and error count.

The checkpoint mode gives the system a vocabulary for queue behavior. In `QUEUE` mode, admitted work is buffered. In `BACKPRESSURE` mode, admission rejects work if remaining capacity is exhausted. In `STREAMLINE` mode, the checkpoint admits without buffering. This makes the checkpoint useful both as an executor queue partition and as a pipeline control point for future streaming or pressure-sensitive designs.

The checkpoint snapshot is a compact operational record. It reports current depth, remaining capacity, oldest wait duration, cumulative admission counters, enqueue and dequeue rates, and downstream health indicators. These values map directly to the queueing interpretation of the scheduler. Depth is backlog. Oldest wait is an early tail-latency warning. Enqueue and dequeue rates expose imbalance between arrival and service. Rejection and cancellation distinguish pressure from successful flow. Downstream latency and errors allow the scheduler to reason beyond the local queue when a policy chooses to do so.

The current scheduler uses checkpoint snapshots for selection rather than for a full control loop. This is a deliberate staging choice. The implementation first establishes the correct entities, partitions, metrics, and decision records. Once those are stable, policies can evolve from priority ordering to pressure-sensitive scheduling without changing the executor contract.

### 5.1 Admission Is a Scheduling Decision Before Selection

It is useful to distinguish admission from selection. Admission decides whether a unit of work may enter a checkpoint. Selection decides which admitted unit should run next. DUUI exposes both, because distributed pipelines fail if they accept unbounded work into a stage whose downstream resources cannot possibly absorb it. A pure selection scheduler can only rearrange backlog after it exists. Admission can prevent backlog from becoming pathological in the first place.

In `QUEUE` mode, DUUI is permissive: accepted items enter the buffer and remain available for selection. This mode is appropriate for ordinary in-memory pipeline execution where the producer rate is bounded by source iteration or by upstream component completion. In `BACKPRESSURE` mode, the checkpoint becomes an explicit capacity gate. This is the mode that can protect storage writers, remote annotator endpoints, GPU-heavy services, or any stage where queue growth is itself a failure mode. In `STREAMLINE` mode, the checkpoint can record admission without acting as a buffer, which is useful for direct flow or future streaming contexts where the carrier should not accumulate backlog.

The admission predicate gives each checkpoint a local correctness boundary. A processor-stage checkpoint can reject malformed tasks. A future policy could reject tasks whose required resource class is unavailable, whose artifact metadata violates a stage contract, or whose expected size exceeds configured limits. This local predicate should remain simple and deterministic. Complex global tradeoffs belong in scheduler policy; checkpoint admission should answer whether the work is structurally acceptable for that checkpoint.

### 5.2 Checkpoint Snapshots as Operational Evidence

The checkpoint snapshot compresses enough information to support both retrospective diagnosis and online policy. Depth indicates immediate backlog. Remaining capacity indicates how close a bounded checkpoint is to rejecting work. Oldest wait is especially important because it gives a tail signal even when average wait is hidden. Accepted and rejected counts separate offered load from successful admission. Enqueued and dequeued counts allow rates. Downstream worker availability, phase latency, and errors connect the local buffer to the service center that follows it.

In a stable system, these values should tell a coherent story. If enqueue and dequeue rates are roughly balanced and oldest wait remains bounded, the stage is keeping up. If enqueue rate exceeds dequeue rate and depth grows, the stage is accumulating work. If depth is low but downstream latency is high, the bottleneck may be inside the component rather than before it. If rejection count rises while worker availability is low, backpressure is active and probably protecting a saturated dependency. If cancellation rises, the pipeline may be responding to failures or shutdown rather than ordinary pressure.

This evidence is also useful for scientific reproducibility. A corpus-processing run should be able to explain not only which components were used, but how the pipeline behaved under load. Two runs with the same code and different scheduler pressure may produce different timeout or retry patterns. Checkpoint metrics give the run a performance provenance layer that can be interpreted alongside annotation outputs.

## 6. Queueing Model and Theoretical Basis

DUUI's scheduler can be understood as a queueing network where stages are service centers and checkpoints are buffers between arrival, readiness, and execution. A document entering a multi-annotator pipeline is not a single isolated request. It becomes a sequence of stage-specific tasks, each with its own service distribution. Remote neural annotators may have high mean latency and high variability. Local UIMA annotators may be CPU-bound and comparatively stable. Containerized services may add cold-start, network, or serialization overhead. DUA import pipelines may shift the bottleneck from annotation to storage writes.

Little's law gives a practical sanity check for any such system. If the arrival rate is stable and the mean time in system increases, the average number of queued or active items must increase. Conversely, if checkpoint depth grows while dequeue rate does not, DUUI is seeing either underprovisioned service, excessive service-time variability, blocked dependencies, or downstream backpressure. The scheduler's use of checkpoint depth and wait metrics is therefore not cosmetic; it is the minimum instrumentation required to connect observed backlog to operational capacity.[^little]

Kingman's formula explains why DUUI should treat saturation as an explicit scheduling objective rather than an after-the-fact failure. The formula approximates mean queue wait for a G/G/1 system as the product of utilization pressure, variability, and mean service time.[^kingman] DUUI workloads are rarely M/M/1 idealizations. Arrivals are often batch-driven, service times vary by document length and model, and distributed services add nonuniform network latency. In such a regime, increasing utilization from moderate to near-full can produce disproportionate wait growth. This is why `DUUISchedulerObjective` includes `CONSTRAIN_UTILIZATION`, `CONSTRAIN_SATURATION`, and `CONSTRAIN_PRESSURE` alongside `MAXIMIZE_THROUGHPUT` and `MAXIMIZE_PROGRESS`.

The default policy does not yet compute Kingman-style estimates. Its value is architectural: all required observables are represented as first-class concepts. A future policy can estimate arrival rate from checkpoint enqueue rate, service rate from dequeue or task execution duration, variability from histograms, and saturation from queue depth growth and active worker counts. It can then decide whether to favor a shorter stage, throttle a pressure-heavy checkpoint, shift blocking work to virtual threads, or wait rather than amplify overload.

### 6.1 Mapping DUUI Terms to Queueing Terms

The correspondence between queueing theory and DUUI is approximate but useful. A checkpoint is not exactly a classical queueing station because a task may also be blocked by dependencies or by component capacity outside the checkpoint. A component is not exactly a single server because it may have replicas, workers, remote service concurrency, and internal model batching. A pipeline is not exactly a Jackson network because document flows may be deterministic through stages, branch in future designs, or fail into retry/quarantine paths. Nevertheless, the vocabulary helps prevent vague performance reasoning.

In DUUI, arrivals are task admissions into checkpoints. Service begins when a worker starts processing the task, although remote components may internally divide service into serialization, transport, model execution, and deserialization. Waiting time is the interval between queue admission and service start or dispatch, depending on the metric boundary. System time is the interval from admission to task completion. Queue length is checkpoint depth plus possibly inflight work, depending on whether the analysis concerns only waiting work or total work in the stage. Utilization is not one number; it may refer to platform executor occupancy, virtual-lane pressure, component slot usage, GPU utilization, remote endpoint concurrency, or storage write saturation.

This multi-resource reality is why DUUI's scheduler objectives are plural. `MAXIMIZE_THROUGHPUT` alone can make a system worse if it drives a scarce remote model into failure. `MINIMIZE_LATENCY` alone can starve expensive but necessary stages. `CONSTRAIN_UTILIZATION` without progress can underuse the system. `CONSTRAIN_PRESSURE` without fairness can protect a hot stage by starving a cold one. The scheduler objective set is therefore a declaration that scheduling is multi-objective from the start, even when the default policy remains simple.

### 6.2 Stability Before Optimality

For DUUI, the first scheduling question is stability: does the pipeline drain work at least as fast as it admits work over the relevant interval? Little's law and Kingman's approximation both point to this priority. A scheduler can be locally optimal for average throughput while still pushing the system into a region where tail wait explodes. This is particularly likely for document pipelines because large artifacts and model-heavy stages introduce high service-time variance.

An optimal-looking configuration may therefore be fragile. For example, increasing virtual dispatch parallelism can improve throughput while downstream endpoints have idle slots. Past the endpoint's real capacity, the same change can increase queue wait, connection contention, timeout rate, and retry load. Similarly, increasing component replicas can improve capacity until the host saturates CPU, memory, GPU, or I/O. The scheduler cannot infer all of this from static configuration. It needs measured feedback: depth, wait, service duration, errors, active workers, and backpressure.

The current architecture creates the feedback channels before encoding an elaborate controller. This is sound engineering. A controller without trustworthy signals becomes guesswork; a simple scheduler with complete signals can be evaluated, debugged, and replaced.

## 7. Policy and Decision Semantics

`DUUIScheduler` receives a `Request` containing checkpoints, capabilities, scheduler objectives, and workload attributes. Null values are normalized: no checkpoints means an empty list, missing capabilities become an empty set, missing objectives become defaults, and missing workload attributes become an empty map. This makes the policy surface robust for callers and tests.

The default `CheckpointPriorityPolicy` follows a clear decision procedure:

1. Inspect every checkpoint snapshot item.
2. Discard null queued tasks and tasks whose dependencies are not ready.
3. Select the highest-priority, earliest-sequence task through natural ordering.
4. If none is ready, return `WAIT` with reason `no checkpoint task is ready`.
5. If one is ready, determine an execution lane and return `ADVANCE`.

The lane selection step is also intentionally simple. A task or stage can set `duui.execution.lane` or `duui.worker.lane`. If the value is `virtual`, the decision selects `DUUIExecutionLane.VIRTUAL`; otherwise it selects `PLATFORM`. If no explicit lane exists, the policy may select a virtual lane when the request advertises a `virtual-thread` capability and the selected task has a blocking phase attribute. Otherwise it preserves the task's requested lane.

The decision record is more than a return value. It is an audit object. It contains the decision timestamp, action, checkpoint id, task id, selected queued task, lane, reason, and attributes. Attributes include scheduler capabilities, objective names, checkpoint ids, workload attributes, selected stage and checkpoint partition metadata, task gid, priority, sequence, resource specification names, observability specification names, and task wait time when available. This design makes scheduling explainable in logs, traces, tests, and future dashboards.

### 7.1 Decision Attributes as a Research Artifact

Decision attributes should be treated as part of the research artifact generated by a pipeline run. In a distributed annotation experiment, the result is not only the output XMI or DUA package. The result also includes the conditions under which that output was produced. If a scheduler selected virtual lanes for blocking stages, if priority caused one corpus partition to advance ahead of another, or if a checkpoint waited because no dependency-ready task existed, those facts explain the temporal behavior of the run.

This is especially important when DUUI is used for comparative research. A benchmark comparing two annotator implementations can be invalidated by hidden scheduling differences. If one implementation runs through platform threads and another through virtual lanes, if one endpoint has backpressure and another does not, or if one component's queue accumulates retries, the apparent model performance may include scheduler artifacts. Decision records make these differences visible.

The attribute model also creates an integration point for later dashboards and reports. A scheduler trace can be grouped by stage gid, checkpoint gid, priority, lane, component, pipeline id, resource specification, or workload class. This is more flexible than baking every grouping into metric names. The decision remains one object, but downstream telemetry and reporting can slice it according to the question being asked.

### 7.2 Policy Replacement Contract

The policy interface is intentionally small: `DUUISchedulerPolicy.decide(Request request)`. That simplicity is a constraint on future complexity. A replacement policy can be sophisticated internally, but it must still consume checkpoint snapshots, capabilities, objectives, and workload attributes, then return a scheduling decision. This keeps policy experimentation from leaking into pipeline submission, executor queue internals, or driver code.

The strongest candidate policies are incremental rather than revolutionary. A pressure-aware policy can start by preserving priority ordering except when a checkpoint exceeds a pressure threshold. An SLO-aware policy can preserve FIFO order within service classes. A resource-aware policy can add lane selection rules before changing task ordering. A fairness policy can add per-corpus quotas without rewriting component execution. Each of these can be evaluated against the default policy because the request and decision surfaces are shared.

The replacement contract also protects tests. Contract tests can assert that a policy returns `WAIT` when no task is ready, that selected tasks are dependency-ready, that selected lanes are legal, and that decision attributes contain required audit keys. Policy-specific tests can then focus on scoring behavior without retesting the entire pipeline.

## 8. Execution Lanes and Worker Model

DUUI separates the scheduler's lane decision from the executor's worker implementation. The pipeline itself extends `ThreadPoolExecutor` with a fixed platform-thread pool. It also owns a virtual-thread-per-task executor. `DUUIQueuedTask.run()` acts as the lane bridge.

Platform-lane execution is appropriate for CPU-bound work or work where thread pinning and platform resource accounting are desired. Virtual-lane execution is appropriate for blocking I/O, remote service calls, or high fan-out workflows where many tasks may wait on external services. The distributed BioFID pipeline note captures the intended pattern: the scheduler selects checkpoint artifacts, while execution behavior is set on stages and component capacity is controlled by v1 replica and slot configuration. In that model, the environment creates addressable resources, but the pipeline owns annotators, nodes, concurrency slots, and task execution.

This matters for DUUI because many annotators are not local functions. A remote v1 component can involve serialization, HTTP transport, server-side model execution, and response deserialization. Blocking the platform executor for every remote wait would reduce scheduler responsiveness. Virtual lanes allow high I/O concurrency without equating each wait with a scarce platform worker. However, virtual threads do not remove downstream limits. Component replicas, per-replica concurrency slots, HTTP endpoints, GPU memory, and storage backends remain real capacity boundaries. The scheduler architecture therefore treats lane choice as one input into execution, not as a substitute for backpressure.

### 8.1 Platform Threads, Virtual Threads, and Workload Character

The distinction between platform and virtual execution lanes is a recognition that DUUI workloads are heterogeneous. CPU-bound local UIMA annotators should not be oversubscribed simply because a virtual-thread executor can create many tasks. A CPU-bound annotator competes for processor time, memory bandwidth, cache, and JVM resources. For that class of work, a bounded platform pool is a useful control mechanism.

Remote and I/O-heavy annotators behave differently. A task may spend much of its lifetime waiting for an HTTP response, a storage read, a container service, or a model server. Keeping a platform worker occupied during that wait can reduce throughput and make the pipeline appear saturated even when the CPU is idle. Virtual threads reduce that mismatch by making blocking waits cheaper at the Java thread level. They are not free capacity; they are a better representation of waiting work.

The scheduler's lane choice should therefore be interpreted as a statement about dominant waiting behavior, not as a performance guarantee. A virtual lane says that the task is expected to block or wait enough that virtual execution is appropriate. A platform lane says that the task should consume bounded executor capacity directly. The correctness of that choice must be verified with execution duration, active worker, and downstream metrics. If a supposedly I/O-bound stage consumes CPU heavily after deserialization, virtual dispatch may only move the bottleneck.

### 8.2 Component Capacity Remains Separate

DUUI components have their own capacity model. A virtualized component may have replicas and workers; a remote endpoint may impose its own concurrency limits; a GPU service may serialize model execution internally. The scheduler must not confuse Java task concurrency with component service capacity. Sending 1,000 virtual-lane tasks toward a service with 16 useful slots is not 1,000-way processing; it is 984 tasks waiting somewhere.

This distinction is why component capacity belongs to drivers and components, while scheduler policy observes pressure. The driver knows how many replicas were requested and which endpoints were created. The component knows its concurrency slots. The worker and telemetry layers know how tasks actually behaved. The scheduler consumes the resulting evidence. This division lets DUUI support Docker, Podman, Kubernetes, Swarm, remote, and local UIMA components without hardcoding all capacity semantics into one scheduler.

## 9. Observability and SLO Alignment

The telemetry catalog gives DUUI a shared language for scheduler health:

- `duui.task.queue.wait` records task waiting time by queue, dispatch mode, and priority.
- `duui.task.execution.duration` records service duration by worker kind, dispatch mode, and stage kind.
- `duui.task.inflight` records active work.
- `duui.executor.queue.depth` and `duui.executor.active.workers` expose executor load.
- `duui.scheduler.decision.duration` measures scheduling overhead.
- `duui.scheduler.backpressure` captures pressure events or ratios.
- `duui.checkpoint.depth` and `duui.checkpoint.wait` expose stage-local backlog and delay.
- `duui.phase.duration` and `duui.phase.errors` connect scheduling outcomes to processing phases.

Google SRE's four golden signals are latency, traffic, errors, and saturation.[^sre-monitoring] DUUI maps latency to queue wait, checkpoint wait, phase duration, and execution duration. It maps traffic to enqueue rate, dequeue rate, task inflight, and processed artifacts. It maps errors to phase errors, downstream checkpoint errors, failed tasks, and failed component calls. It maps saturation to checkpoint depth, executor queue depth, active workers, worker availability, component borrow pressure, and backpressure.

Service level objectives make these metrics operational rather than decorative. The SRE book argues that publishing SLOs sets performance expectations and prevents users and service owners from inventing incompatible assumptions about reliability or speed.[^sre-slo] DUUI scheduling can use the same discipline. For a research pipeline, an SLO might specify that 95 percent of documents should complete a given stage within a time budget, that checkpoint wait should stay below a defined threshold during steady-state processing, or that failed artifact ratio must remain under a configured bound. For import pipelines, an SLO may be expressed as documents per second plus maximum tolerated rejection or retry rate. These objectives can be attached to `DUUISchedulerObjectives` and workload attributes before policy implementations become more adaptive.

An important implication is that DUUI should avoid paging or alerting merely because a metric looks unusual. SRE guidance warns against alerting simply because something seems odd; alerts should correspond to real or imminent user-visible problems.[^sre-monitoring] For DUUI, the analogous discipline is to distinguish exploratory dashboards from action-triggering scheduler pressure. A transient checkpoint spike during batch startup is evidence, not automatically failure. A sustained wait increase combined with flat dequeue rate, high active workers, and rising downstream errors is stronger evidence of saturation.

### 9.1 Concrete SLOs for DUUI Scheduling

SLOs for DUUI should be stated in terms of user-visible or research-visible outcomes, not raw implementation details alone. A raw metric such as executor queue depth is useful, but it is not an objective by itself. A better SLO connects that depth to completion behavior, freshness, or correctness. For example, a batch annotation SLO might require that 99 percent of accepted documents complete the full pipeline within a configured wall-clock budget for a given corpus size and component profile. A stage SLO might require that the 95th percentile checkpoint wait for each processor stage remains below a threshold during steady state. An import SLO might require sustained document ingestion above a minimum rate with zero unhandled storage failures and a bounded retry ratio.

These objectives can be translated into scheduler evidence. Completion budgets depend on queue wait plus execution duration across stages. Stage wait objectives depend on checkpoint wait histograms and oldest-wait snapshots. Ingestion objectives depend on enqueue/dequeue rates, failure counters, and backpressure. Error-ratio objectives depend on phase errors and task failure state. Saturation objectives depend on checkpoint depth, executor queue depth, active workers, and downstream availability.

SLOs also clarify what the scheduler should not optimize. If a pipeline's primary objective is reproducible completion of a research corpus, minimizing mean latency for the first few documents may be irrelevant. If a service is interactive, tail latency may matter more than total batch throughput. If a DUA import is storage-bound, increasing annotator parallelism may be counterproductive. Explicit objectives prevent the scheduler from chasing generic speed when the actual system goal is bounded wait, stable throughput, or low failure ratio.

### 9.2 Dashboards and Retrospective Analysis

A useful DUUI scheduler dashboard should answer a small set of questions. Is work arriving? Is work being served? Where is it waiting? Are workers active? Are errors rising? Is any checkpoint near capacity? Are scheduling decisions taking significant time? Are virtual lanes carrying blocking work or hiding CPU saturation? These questions map naturally onto the metric catalog and the decision attributes.

For retrospective analysis, the dashboard should preserve temporal relationships. A rise in checkpoint depth is more meaningful when plotted with dequeue rate, execution duration, and phase errors for the same stage. A rise in scheduler backpressure is more meaningful when plotted with downstream latency and worker availability. A sudden drop in throughput may be explained by a single component endpoint, a storage backend, a source stall, or a dependency gate. Without stage and checkpoint labels, all of those collapse into "the pipeline is slow."

The SRE distinction between white-box and black-box monitoring is useful here. DUUI's scheduler metrics are white-box: they expose internal queue and worker state. A black-box view would measure externally visible run progress, such as documents completed per minute or time to first output. Both are needed. White-box metrics explain why; black-box metrics prove whether the system is meeting the user's expectation.

## 10. Distributed Runtime Integration

DUUI scheduling operates above several runtime drivers. Docker, Podman, Swarm, Kubernetes, remote HTTP, and UIMA drivers instantiate components and expose annotator resources. The scheduler should not need to know whether a component endpoint came from a local container, a Kubernetes service, or a fixed remote URL. It should see a task, a stage, capacity-related attributes, and observability evidence.

This abstraction is visible in the pipeline builder. V1 components are added through driver-backed initializers. Starting a pipeline instantiates components and registers them as resources. Processor stages are created with checkpoints, while drivers remain responsible for deployment and component construction. Tests confirm that virtualization drivers can instantiate multiple replicas and derive concurrency slots from replicas and workers. The scheduler sees the resulting stage tasks rather than the container lifecycle itself.

For distributed workflows, this is a useful separation of concerns:

- Drivers create reachable annotator resources.
- Components model replicas and concurrency slots.
- Stages define processing order and checkpoint partitioning.
- Checkpoints admit and expose stage-local work.
- The scheduler selects ready work and assigns an execution lane.
- Workers execute tasks and report task state.
- Telemetry records whether the resulting system is meeting operational objectives.

The architecture therefore supports multiple deployment modes without multiplying scheduler implementations. A Kubernetes run and a Podman run can use the same scheduling policy while differing in endpoint discovery, resource lifecycle, and cluster-level placement.

### 10.1 Scheduling Across Local, Remote, and Virtualized Components

Local UIMA components are the simplest case. Service time is mostly local CPU and memory work, and failures are usually semantic or programming failures. The scheduler can rely on platform thread limits and task state to understand progress. Remote components introduce network latency, endpoint health, and serialization overhead. Virtualized components add image lifecycle, container startup, service discovery, replica count, and worker-slot configuration. Kubernetes and Swarm add a cluster scheduler below DUUI's scheduler.

DUUI's scheduler deliberately sits above all of these. It does not replace Kubernetes placement, container orchestration, or HTTP client behavior. Instead, it schedules DUUI tasks that may happen to call into those systems. This is important because lower-level schedulers optimize different resources. Kubernetes schedules pods onto nodes; it does not know the semantic priority of a UIMA artifact. The JVM executor schedules runnables; it does not know which checkpoint belongs to a slow taxonomic stage. A remote model server may schedule GPU batches; it does not know the upstream corpus SLO. DUUI scheduling is the layer that sees pipeline semantics.

The same architecture also avoids tying DUUI to one deployment assumption. A research group can start with local components, move heavy annotators to Podman, deploy shared services on Kubernetes, and still use the same pipeline-level scheduling vocabulary. The scheduler request remains about checkpoints, capabilities, objectives, and workload attributes. Deployment changes alter the evidence and capacity model, not the core scheduling contract.

### 10.2 Interaction With DUA and Storage-Heavy Pipelines

The DUA storage work changes the scheduling discussion because the bottleneck may move from annotation to persistence and query-oriented materialization. A raw XMI import pipeline may need many readers, batch splitting, materialization, and write chunks. The scheduler still sees tasks and checkpoints, but downstream pressure may come from LMDB writes, Postgres transactions, package snapshots, or type-system indexing rather than annotator endpoints.

For storage-heavy pipelines, backpressure becomes more important. If readers produce materialized write units faster than the backend can commit them, an unbounded queue can consume memory and produce poor tail behavior. Checkpoint capacity, rejection counters, oldest wait, and dequeue rates are the evidence needed to tune chunk size and reader parallelism. Little's law is especially practical here: if write-stage depth grows while write throughput remains flat, more readers are not improving end-to-end throughput; they are increasing waiting work.

This is another reason the scheduler should remain artifact- and checkpoint-centered rather than annotator-centered. DUUI pipelines are not only chains of NLP models. They can be importers, exporters, query materializers, transport bridges, or hybrid workflows. Checkpoints and tasks generalize across those cases.

## 11. Failure, Backpressure, and Dependency Readiness

Scheduling is also a failure-containment problem. DUUI task states include queued, scheduled, running, blocked, retrying, quarantined, completed, failed, and cancelled. Checkpoints separately count accepted, rejected, cancelled, enqueued, and dequeued work. This split allows the system to distinguish an admission failure from an execution failure.

Backpressure appears first at admission. In `BACKPRESSURE` mode, a checkpoint rejects new work if its buffer has no remaining capacity. Even in unbounded queue mode, downstream indicators can report low worker availability, rising latency, or errors. Today those indicators are exposed for policy use; future pressure-aware scheduling can refuse to advance work into a saturated stage, favor stages that drain critical backlog, or shift blocking stages to virtual lanes where appropriate.

Dependency readiness is enforced twice. The default scheduler filters queued tasks by `dependenciesReady()`. The checkpoint's internal `pollReady()` also skips dependency-blocked worker tasks. This redundancy is useful because checkpoints are general carriers, not only scheduler partitions. A dependency-blocked task should not be selected merely because it is older or higher priority.

### 11.1 Failure Semantics and Scheduler Boundaries

The scheduler should not classify every failure. DUUI has separate exception and failure-context surfaces for semantic data failures, transient infrastructure failures, persistent infrastructure failures, timeouts, resource exhaustion, and programming bugs. The scheduler's responsibility is narrower: avoid selecting impossible work, avoid amplifying pressure when evidence says downstream service is unhealthy, and preserve enough decision context that failures can be interpreted.

A failed task may imply different scheduler responses depending on context. A semantic failure in one artifact should not necessarily reduce stage concurrency. A burst of transient HTTP failures may justify slowing or redirecting a checkpoint. A persistent storage failure may justify backpressure or stopping admission. A programming bug may require quarantine rather than retry. The default policy does not implement these distinctions, but the decision model leaves room for them.

The key boundary is that scheduling should react to classified evidence, not raw exceptions sprayed through the system. Workers, drivers, and failure classifiers should produce meaningful state. Scheduler policy can then use that state as pressure, availability, or objective evidence. This keeps the scheduler from becoming a catch-all error handler.

### 11.2 Backpressure as a Positive Mechanism

Backpressure is often described as a failure symptom, but in DUUI it should be treated as a positive control mechanism. A checkpoint that rejects work before memory exhaustion is doing its job. A scheduler that waits rather than advancing work into a saturated endpoint is preserving the rest of the pipeline. A source that slows because downstream storage is full is preventing data loss or cascading failure.

This view aligns with SRE thinking about saturation and alert quality. Saturation is not only "the system is full"; it is a signal that the system is nearing a limit that affects service quality. In DUUI, pressure-aware scheduling can turn that signal into controlled behavior. The desired outcome is not zero backpressure. The desired outcome is bounded backpressure that appears early enough to protect correctness and disappears when downstream capacity recovers.

## 12. Measurement and Evidence Model

The codebase already contains contract tests that lock down the scheduling shape. `DUUICoreReplacementContractTest` verifies that the scheduler prioritizes checkpoint tasks and selects a worker lane, that operational types are concrete implementations where expected, and that the pipeline is a `ThreadPoolExecutor` with stage partitions. `DUUIBaseReplacementContractTest` verifies checkpoint snapshots, dispatch modes, metric catalog entries, and required phase events. These tests are not a full performance study; they are architectural evidence that the scheduling contract exists and is enforced.

End-to-end scheduler characterization should add workload experiments that vary four independent factors:

1. Arrival rate: batch size, source speed, and burstiness.
2. Service distribution: fast local annotators, slow remote annotators, and high-variance model calls.
3. Capacity: platform threads, virtual dispatch parallelism, replicas, workers, and per-replica concurrency slots.
4. Failure and pressure: transient endpoint failures, high downstream latency, checkpoint capacity limits, and storage write contention.

The primary dependent variables should match the telemetry catalog: queue wait, checkpoint depth, enqueue/dequeue rate, execution duration, active workers, inflight tasks, scheduler decision duration, backpressure, phase errors, and end-to-end runtime. Little's law can be used as a consistency check between depth, throughput, and observed waiting time. Kingman's formula can be used as an interpretive model when increased utilization and variability produce nonlinear queue delay.

### 12.1 Workload Families for Scheduler Characterization

Scheduler characterization should cover at least four workload families. The purpose is not to close this document with a benchmark verdict, but to describe what evidence a future implementation or paper would need before claiming that a policy improves the architecture.

The first family is a CPU-bound local pipeline. It should use local UIMA or lightweight in-process annotators with controlled service times. The purpose is to verify that platform thread limits behave predictably, that priority and sequence ordering are preserved, and that queue wait follows expected utilization changes.

The second family is an I/O-bound remote pipeline. It should use remote endpoints or controlled test services with configurable latency distributions and failure rates. The purpose is to evaluate virtual-lane behavior, downstream latency evidence, retry pressure, and the relationship between virtual dispatch and real endpoint capacity.

The third family is a mixed BioFID-style pipeline. It should include several annotators with different service profiles, such as spaCy, GeoNames, GNFinder, and TaxoNERD. The purpose is to test whether checkpoint metrics identify the actual bottleneck stage and whether scheduler decisions remain explainable when stages have unequal cost.

The fourth family is a storage-heavy DUA import or export pipeline. It should vary reader count, write chunk size, backend durability, and storage backend. The purpose is to test whether admission and backpressure protect memory and whether throughput is bounded by storage service rate rather than source parallelism.

Each family should be run under at least three capacity regimes: underloaded, near saturation, and overloaded. Underloaded runs prove the scheduler does not add excessive overhead. Near-saturation runs expose nonlinear wait growth. Overloaded runs test backpressure and failure containment. For each regime, the measurement report should include not only aggregate runtime but also per-stage queue wait, depth, dequeue rate, execution duration, error rate, and scheduler decision duration.

### 12.2 Evidence Required for Scheduler Claims

A scheduler claim should avoid relying on a single green run. A run that completes can still hide unfairness, unstable queue growth, or saturated downstream services. Strong evidence should include:

- a reproducible workload description;
- component configuration, including replicas and workers;
- platform and virtual dispatch settings;
- per-stage metric summaries;
- tail wait, not only mean wait;
- failure and retry counts;
- backpressure counts or ratios;
- output correctness checks;
- comparison against a baseline policy or configuration;
- raw artifacts sufficient to replot queue depth and throughput over time.

This evidence standard matters because DUUI scheduling is both an engineering and research concern. A scheduler that improves mean throughput while increasing failure ratio may be unacceptable. A scheduler that improves a small benchmark but hides stage starvation may fail on real corpora. Measurement must therefore connect performance, correctness, and operational stability.

## 13. Research and Engineering Outlook

The current scheduler is intentionally policy-minimal, but the architecture supports richer research directions.

A pressure-aware policy could compute a score per checkpoint from depth, oldest wait, dequeue rate, downstream latency, and error rate. Such a policy would still return the same `DUUISchedulingDecision`, preserving the executor contract.

An SLO-aware policy could accept workload attributes such as deadline class, expected stage cost, or target percentile latency. It could then prefer work that protects an error budget or prevents tail-latency collapse.

A variability-aware policy could estimate service-time variation per stage and avoid driving high-variance stages into near-saturation. This would directly apply the intuition behind Kingman's approximation.

A resource-aware policy could combine component slot availability, GPU capability, memory pressure, and storage pressure with task attributes. This would allow DUUI to decide not only which task is next, but whether advancing that task now would degrade the rest of the pipeline.

A fairness policy could provide per-corpus, per-stage, or per-priority isolation. This matters when several corpora share annotator infrastructure or when interactive workloads coexist with batch imports.

The most important constraint is that future policies should preserve decision explainability. Every adaptive choice should remain inspectable through decision attributes and metric dimensions. A smart scheduler that cannot explain itself would be a poor fit for reproducible research pipelines.

### 13.1 Toward Adaptive Scheduling

Adaptive scheduling should proceed in stages. The first stage is measurement-only: collect checkpoint, worker, and task metrics without changing decisions. The second stage is advisory: compute pressure scores or SLO risk scores and expose them in decision attributes while still using the default ordering. The third stage is bounded intervention: allow the policy to change lane selection or defer work only when scores cross explicit thresholds. The fourth stage is full adaptive ordering, where policy can reorder across checkpoints based on pressure, fairness, and SLO risk.

This staged path reduces risk. It gives operators and researchers time to compare predicted pressure with observed outcomes. It also prevents the scheduler from becoming a source of surprising nondeterminism before the metrics are trusted. In research pipelines, reproducibility is a feature; adaptation must be explainable enough that a run can be interpreted after the fact.

### 13.2 Open Research Questions

Several research questions remain open.

How should DUUI estimate service-time distributions for stages with small sample sizes or rapidly changing input composition? A taxonomic recognizer may behave differently on short abstracts and long OCR documents. A single moving average may be too crude.

How should DUUI balance fairness and throughput when corpora share infrastructure? Strict fairness can reduce throughput if one corpus uses a slow stage. Pure throughput can starve smaller or lower-priority work.

How should DUUI represent deadlines? A document-level deadline may not map cleanly to stage-local decisions if later stages are more expensive than earlier stages. Deadline-aware scheduling may need predicted remaining cost.

How should DUUI coordinate with lower-level schedulers? Kubernetes may reschedule pods, GPU servers may batch internally, and HTTP clients may retry. DUUI needs enough feedback to avoid fighting those systems.

How should scheduler decisions be persisted for long-running corpus jobs? Fine-grained traces can be large, but compressed summaries may hide the exact decision path needed for reproducibility. This is a storage and observability design question as much as a scheduling question.

These questions suggest that the current architecture is a foundation for research rather than the endpoint. Its value is that it makes the core objects explicit: checkpoints, tasks, decisions, lanes, objectives, and metrics. Once those objects are stable, scheduling research can proceed without repeatedly changing the pipeline's basic execution contract.

## 14. Citation-Driven Design Commitments

The design commitments that follow from the cited literature are concrete.

From Little and Kleinrock, DUUI should continue treating depth, wait, arrival rate, and service rate as nonoptional scheduling evidence.[^little][^kleinrock] A pipeline that cannot report checkpoint depth and wait cannot explain whether it is stable.

From Kingman and Dean/Barroso, DUUI should track tail behavior and variability, not only means.[^kingman][^tail] The architecture should make p95/p99 wait and execution duration natural dashboard outputs, especially for multi-stage and fan-out-style workflows.

From MapReduce, Mesos, Omega, Borg, and Kubernetes, DUUI should keep semantic pipeline scheduling separate from cluster or container placement.[^mapreduce][^mesos][^omega][^borg][^bok] Lower layers manage machines, pods, containers, and services; DUUI manages artifact-stage progress.

From DRF, DUUI should avoid pretending that a single worker count captures real resource fairness.[^drf] Future policies need a way to describe CPU, GPU, memory, network, endpoint, and storage shares.

From Sparrow and the power-of-two-choices literature, DUUI should keep randomized and decentralized scheduling options open if a single pipeline scheduler becomes too centralized for large deployments.[^sparrow][^twochoices] The current checkpoint policy is intentionally simple, but the request/decision contract should not prevent later low-latency sampling designs.

From SRE monitoring, SLOs, and overload handling, DUUI should define scheduler health in terms of latency, traffic, errors, saturation, explicit objectives, and graceful pressure response.[^sre-monitoring][^sre-slo][^sre-overload] The scheduler should be able to wait, shed, or backpressure work as a designed behavior, not only as an emergency reaction.

These commitments keep the document open-ended by design. The architecture is a research surface for scheduling policies, not a completed claim that one policy is optimal.

## References

[^little]: John D. C. Little, "A Proof for the Queuing Formula: L = (lambda) W," *Operations Research*, vol. 9, no. 3, pp. 383-387, June 1961. DOI: 10.1287/opre.9.3.383. Source metadata: https://ideas.repec.org/a/inm/oropre/v9y1961i3p383-387.html

[^sre-monitoring]: Rob Ewaschuk, "Monitoring Distributed Systems," in *Site Reliability Engineering*, Google. https://sre.google/sre-book/monitoring-distributed-systems/

[^sre-slo]: Vivek Rau, Betsy Beyer, and Niall Richard Murphy, "Service Level Objectives," in *Site Reliability Engineering*, Google. https://sre.google/sre-book/service-level-objectives/

[^kingman]: "Kingman's formula," Wikipedia, accessed June 12, 2026. https://en.wikipedia.org/wiki/Kingman%27s_formula

[^kleinrock]: Leonard Kleinrock, *Queueing Systems, Volume 1: Theory*, Wiley-Interscience, 1975. ACM bibliographic record: https://dl.acm.org/doi/book/10.5555/1096491

[^tail]: Jeffrey Dean and Luiz Andre Barroso, "The Tail at Scale," *Communications of the ACM*, vol. 56, no. 2, pp. 74-80, 2013. DOI: 10.1145/2408776.2408794. Google Research record: https://research.google/pubs/the-tail-at-scale/

[^sre-overload]: Alejandro Forero Cuervo, "Handling Overload," in *Site Reliability Engineering*, Google. https://sre.google/sre-book/handling-overload/

[^drf]: Ali Ghodsi, Matei Zaharia, Benjamin Hindman, Andy Konwinski, Scott Shenker, and Ion Stoica, "Dominant Resource Fairness: Fair Allocation of Multiple Resource Types," NSDI 2011. https://www.usenix.org/conference/nsdi11/dominant-resource-fairness-fair-allocation-multiple-resource-types

[^sparrow]: Kay Ousterhout, Patrick Wendell, Matei Zaharia, and Ion Stoica, "Sparrow: Distributed, Low Latency Scheduling," SOSP 2013. DOI: 10.1145/2517349.2522716. https://dl.acm.org/doi/10.1145/2517349.2522716

[^omega]: Malte Schwarzkopf, Andy Konwinski, Michael Abd-El-Malek, and John Wilkes, "Omega: Flexible, Scalable Schedulers for Large Compute Clusters," EuroSys 2013. Google Research record: https://research.google/pubs/omega-flexible-scalable-schedulers-for-large-compute-clusters/

[^borg]: Abhishek Verma, Luis Pedrosa, Madhukar R. Korupolu, David Oppenheimer, Eric Tune, and John Wilkes, "Large-scale Cluster Management at Google with Borg," EuroSys 2015. Google Research record: https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/

[^bok]: Brendan Burns, Brian Grant, David Oppenheimer, Eric Brewer, and John Wilkes, "Borg, Omega, and Kubernetes," *Communications of the ACM*, 2016. Google publication PDF: https://research.google.com/pubs/archive/44843.pdf

[^mapreduce]: Jeffrey Dean and Sanjay Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters," OSDI 2004. https://www.usenix.org/conference/osdi-04/mapreduce-simplified-data-processing-large-clusters

[^mesos]: Benjamin Hindman, Andy Konwinski, Matei Zaharia, Ali Ghodsi, Anthony D. Joseph, Randy Katz, Scott Shenker, and Ion Stoica, "Mesos: A Platform for Fine-Grained Resource Sharing in the Data Center," NSDI 2011. https://www.usenix.org/conference/nsdi11/mesos-platform-fine-grained-resource-sharing-data-center

[^twochoices]: Michael D. Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing," PhD thesis, University of California, Berkeley, 1996. https://www2.eecs.berkeley.edu/Pubs/TechRpts/1996/7979.html

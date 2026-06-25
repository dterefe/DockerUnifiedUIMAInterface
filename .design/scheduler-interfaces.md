# Scheduler System — Comprehensive Interface & Class Specification

> **Principle**: Everything is implemented EXCEPT stage component subclasses.
> The user writes procedural dependency declarations; the framework handles
> ordering, concurrency, pooling, backpressure, and telemetry.

---

## Target Ergonomics

```java
var pipeline = Pipeline.define("biofid-import")
    .source(from -> from
        .discover("/data/xmi", "*.xmi.gz")
        .parallelism(4))
    .stage("artifacts", ArtifactAnnotator::new)
        .dependsOn("source")
        .concurrency(Platform.of(8))
    .stage("taxa", TaxonAnnotator::new)
        .dependsOn("artifacts")
        .concurrency(Slot.shared())          // thread-bound
    .stage("embeddings", EmbeddingAnnotator::new)
        .dependsOn("taxa")
        .concurrency(Pool.of(4))
        .timeout(Duration.ofSeconds(30))
        .retry(3, ExponentialBackoff.of(100, TimeUnit.MILLISECONDS))
    .target(to -> to
        .write("/data/output", Format.XMI))
    .onFailure(FailureAction.SKIP_AND_CONTINUE)
    .withTelemetry(Telemetry.toFile("/tmp/telemetry.jsonl"))
    .build();

pipeline.run();
```

---

## INTERFACES — Contracts

### Work<T>
```
A unit of computation. Has identity, dependencies, state, and a payload.

- id(): String
- state(): WorkState { CREATED, SCHEDULED, RUNNING, COMPLETED, FAILED, CANCELLED }
- dependencies(): List<Work<?>>        // must complete before this runs
- dependents(): List<Work<?>>          // cannot start until this completes
- payload(): Supplier<T>               // the actual computation
- onComplete(Consumer<T>): Work<T>     // callback chain
- onFailure(Consumer<Throwable>): Work<T>
```

### StageDefinition
```
Describes a pipeline stage before it's built. Declared by user, resolved by builder.

- name(): String
- component(): Class<?>                // the processing component
- concurrency(): ConcurrencyModel      // how many concurrent instances
- dependencies(): List<String>         // stage names this depends on
- timeout(): Duration                  // per-work timeout
- retryPolicy(): RetryConfig           // retry configuration
- failureAction(): FailureAction       // what to do when work fails
```

### ConcurrencyModel (sealed)
```
How a stage handles concurrent work.

sealed interface permits Slot, Pool, Platform, Virtual, Unbounded

- Slot         → 1 concurrent, FIFO ordered
- Pool.of(n)   → n concurrent, any order
- Platform.of(n) → n OS threads
- Virtual      → 1 virtual thread per work item
- Unbounded    → no limit
```

### SchedulingPolicy
```
Decides which checkpoint to dequeue from next.

- select(CheckpointSnapshot...): int   // returns index of chosen checkpoint, or -1 for WAIT
- name(): String
```

### ExecutionLane (sealed)
```
Where work runs.

sealed interface permits PlatformLane, VirtualLane

- PlatformLane(nThreads) → fixed thread pool
- VirtualLane            → virtual thread per task
```

### TelemetrySink
```
Lateral service entry point. Receives structured events from the pipeline.

- onWorkStarted(Work<?>)
- onWorkCompleted(Work<?>, Duration elapsed)
- onWorkFailed(Work<?>, Throwable, Duration elapsed)
- onStageStarted(String stageName)
- onStageCompleted(String stageName)
- onCheckpointSnapshot(String stageName, int depth, double availability)
- onPipelineStarted()
- onPipelineCompleted(Duration total)
- onPipelineFailed(Throwable)
```

### RetryConfig
```
- maxAttempts: int
- backoff: BackoffStrategy
- retryOn: Set<Class<? extends Throwable>>   // which exceptions to retry
```

### BackoffStrategy (sealed)
```
sealed interface permits Fixed, Linear, Exponential, Jittered

- nextDelay(attempt: int): Duration
```

---

## CLASSES — Fully Implemented

### Pipeline
```
The orchestrator. Created by PipelineBuilder. Owns the dependency graph,
checkpoints, stages, executor, and scheduler. The only entry point the
user interacts with after definition.

FIELDS:
  - stages: Map<String, Stage>           // name → runtime stage
  - checkpoints: Map<String, Checkpoint> // name → input queue
  - graph: DependencyGraph               // stage ordering
  - executor: ExecutorPool               // platform + virtual threads
  - scheduler: SchedulingPolicy          // pluggable
  - telemetry: List<TelemetrySink>       // lateral service entry points
  - failureAction: FailureAction         // default for all stages

METHODS:
  - define(name): PipelineBuilder        // static entry point
  - run(): PipelineResult                // execute the entire pipeline
  - runAsync(): CompletableFuture<PipelineResult>
  - shutdown(): void                     // graceful shutdown
```

### PipelineBuilder
```
Fluent builder. Accumulates StageDefinitions, validates the DAG,
resolves dependencies into execution order, creates the Pipeline.

METHODS:
  - source(config): PipelineBuilder
  - stage(name, component): StageBuilder
  - target(config): PipelineBuilder
  - onFailure(action): PipelineBuilder
  - withTelemetry(sink): PipelineBuilder
  - withScheduler(policy): PipelineBuilder
  - build(): Pipeline
```

### StageBuilder (inner of PipelineBuilder)
```
Fluent builder for a single stage. Returned by pipeline.stage().

METHODS:
  - dependsOn(String...): StageBuilder
  - concurrency(ConcurrencyModel): StageBuilder
  - timeout(Duration): StageBuilder
  - retry(int, BackoffStrategy): StageBuilder
  - onFailure(FailureAction): StageBuilder
  - and(): PipelineBuilder               // return to pipeline builder
```

### Checkpoint
```
Thread-safe FIFO between two stages. One checkpoint per stage (the stage's
input queue). The source's checkpoint is pre-filled during discovery.

IMPLEMENTATION DETAILS:
  - Internally: ArrayDeque<T> + ReentrantLock + Condition
  - accept(work): append, signal notEmpty, increment counters
  - poll(): remove first, increment dequeued, return or null
  - hasReady(): boolean (non-blocking)
  - snapshot(): CheckpointSnapshot
  - Backpressure: when mode=BACKPRESSURE, accept() blocks if queue full.
    When mode=STREAMLINE, accept() rejects if downstream saturated.
    When mode=QUEUE, accept() never blocks.

CONFIGURABLE:
  - mode: BACKPRESSURE | QUEUE | STREAMLINE
  - capacity: int (for BACKPRESSURE threshold)
```

### CheckpointSnapshot
```
Immutable snapshot of checkpoint state at an instant. Used by SchedulingPolicy
to make decisions without locking.

FIELDS:
  - depth: int                    // current queue depth
  - oldestWait: Duration          // time since oldest accepted item
  - accepted: long                // total accepted count
  - dequeued: long                // total dequeued count
  - downstreamAvailability: double // 0.0 = saturated, 1.0 = idle
  - downstreamLatency: Duration   // rolling average processing time
  - downstreamErrors: long        // error count downstream
```

### Stage
```
Runtime stage. Wraps a component with its input checkpoint, concurrency
model, retry config, and failure action.

INTERNAL FLOW:
  1. Dequeue work from input checkpoint
  2. Acquire concurrency slot (block if Slot/Pool exhausted)
  3. Execute work.payload() with timeout
  4. On success: pass result to downstream checkpoint(s)
  5. On failure: apply retry policy, or failure action
  6. Release concurrency slot
  7. Update downstream signals on output checkpoint

The user NEVER instantiates Stage directly — PipelineBuilder creates them.
```

### ExecutorPool
```
Manages platform and virtual thread pools. Dispatches work based on
the stage's concurrency model.

INTERNAL:
  - platformPool: ExecutorService (fixed, CPU count or configured)
  - virtualPool: ExecutorService (virtual-thread-per-task)

METHODS:
  - dispatch(Work<?>, ConcurrencyModel): void
  - shutdown(): void
```

### DependencyGraph
```
Computes execution order from declared stage dependencies.

INPUT:  Map<String, Set<String>>  // stageName → {dependsOn...}
OUTPUT: List<List<Stage>>         // topological levels — stages at same
                                   // level can run in parallel

ALGORITHM:
  1. Build adjacency list from dependency declarations
  2. Detect cycles → throw at build() time (not runtime)
  3. Topological sort into levels
  4. Source stage is always level 0
  5. Stages that depend only on source are level 1 (can run in parallel)
  6. Stages with multi-level chains are placed at the deepest required level
```

### DefaultSchedulingPolicy
```
Selects the checkpoint with the deepest queue. Ties broken by oldest wait time.

ALGORITHM:
  1. Filter to snapshots with depth > 0
  2. Sort by: depth DESC, oldestWait ASC
  3. Return index 0, or -1 if none ready
```

### Alternative Policies (all implement SchedulingPolicy)

- **RoundRobinPolicy**: cycles through ready checkpoints in order
- **WeightedFairSharePolicy**: weights per stage, proportional dequeue allocation
- **ShortestProcessingTimePolicy**: prefers lowest downstreamLatency
- **LowestErrorRatePolicy**: prefers lowest downstreamErrors / dequeued
- **BackpressureAwarePolicy**: deprioritizes downstreamAvailability < threshold
- **CompositePolicy**: chains policies: primary → tiebreaker → validator
- **AdaptivePolicy**: switches strategy based on runtime metrics

### FailureAction (enum)
```
What happens when a Work item fails after all retries.

- FAIL_FAST: stop entire pipeline
- SKIP_AND_CONTINUE: skip this work, continue with next
- RETRY_INDEFINITELY: keep retrying (dangerous)
- QUARANTINE_STAGE: stop this stage, continue others
```

### PipelineResult
```
Result of a pipeline run.

- totalWork: long
- completed: long
- failed: long
- skipped: long
- duration: Duration
- stageStats: Map<String, StageStats>
- telemetry: List<TelemetryEvent>
```

### StageStats
```
Per-stage statistics.

- name: String
- completed: long
- failed: long
- skipped: long
- avgLatency: Duration
- p50Latency: Duration
- p99Latency: Duration
```

---

## LATERAL SERVICE INTEGRATION

The pipeline emits events through pluggable `TelemetrySink` instances.
These are lateral — they observe but never control. The pipeline does not
know what sinks do.

Examples of what sinks can be:
- File sink: writes JSONL to disk
- Prometheus sink: exposes /metrics endpoint
- Log sink: writes to SLF4J / JUL
- Kafka sink: publishes events to a topic
- WebSocket sink: streams live progress to a dashboard

Each sink receives:
- Work lifecycle events (started, completed, failed)
- Stage lifecycle events (started, completed)
- Checkpoint snapshots (depth, availability, latency)
- Pipeline lifecycle events (started, completed, failed)

---

## DEPENDENCY OFFLOADING

The user declares dependencies by name. The framework computes execution
order, parallelism, and backpressure automatically.

```
User writes:
  .stage("B", ...).dependsOn("A")
  .stage("C", ...).dependsOn("A")
  .stage("D", ...).dependsOn("B", "C")

Framework computes:
  Level 0: [A]           ← source
  Level 1: [B, C]        ← can run in parallel (both depend only on A)
  Level 2: [D]           ← must wait for both B and C

Work flows:
  A produces → B's checkpoint + C's checkpoint (fan-out)
  B produces → D's checkpoint
  C produces → D's checkpoint
  D's checkpoint merges both streams (fan-in)
```

The framework automatically:
- Creates one checkpoint per stage (the stage's input queue)
- Routes work from producer output to consumer input checkpoints
- Handles fan-out (one producer → many consumers)
- Handles fan-in (many producers → one consumer via shared checkpoint)
- Applies backpressure when any consumer's checkpoint is full
- Schedules across checkpoints based on the configured policy

---

## CONCURRENCY & POOLING — Automatic

The user only declares the concurrency model per stage. Everything else is automatic.

```
.concurrency(Slot.shared())
  → Framework creates a Semaphore(1) for this stage
  → Only one work item processes at a time
  → Subsequent work waits in the checkpoint

.concurrency(Pool.of(4))
  → Framework creates a Semaphore(4)
  → Up to 4 work items process concurrently
  → 5th item blocks until one completes

.concurrency(Virtual)
  → Framework uses virtual threads (unbounded)
  → Each work item gets its own virtual thread

.concurrency(Platform.of(8))
  → Framework uses a fixed platform thread pool of 8
```

The framework:
- Creates the semaphore/pool at build() time
- Acquires before executing work
- Releases after work completes (success or failure)
- The user never sees threads, semaphores, or executors

---

## CHECKPOINT BACKPRESSURE — Automatic

When a stage is slow, its input checkpoint fills up. The framework
automatically propagates this upstream.

```
Stage B is slow (100ms per item). Stage A is fast (1ms per item).

Without backpressure:
  A produces 1000 items → B's checkpoint has 1000 waiting
  Memory grows unbounded. Eventually OOM.

With backpressure (QUEUE mode, default):
  A keeps producing. B's checkpoint grows. No OOM unless truly infinite.

With backpressure (BACKPRESSURE mode):
  B's checkpoint reaches capacity (e.g., 100).
  A calls checkpoint.accept() → blocks.
  B completes one item → checkpoint signals → A wakes, produces one more.
  Queue depth stays at exactly 100. Memory is bounded.

With backpressure (STREAMLINE mode):
  B's checkpoint signals downstreamAvailability = 0.0 when near capacity.
  Scheduler reads snapshot → deprioritizes A (its downstream is saturated).
  Other stages get priority. A stalls until B drains.
```

---

## COMPLETE INTERFACE SUMMARY

```
INTERFACES (contracts — 7):
  Work<T>              — unit of computation
  StageDefinition      — description declared by user  
  ConcurrencyModel     — sealed: Slot | Pool | Platform | Virtual | Unbounded
  SchedulingPolicy     — checkpoint selection strategy
  ExecutionLane        — sealed: PlatformLane | VirtualLane
  TelemetrySink        — lateral observer
  BackoffStrategy      — sealed: Fixed | Linear | Exponential | Jittered

CLASSES (fully implemented — 15):
  Pipeline             — orchestrator, owns everything
  PipelineBuilder      — fluent builder, validates DAG
  StageBuilder         — fluent per-stage builder
  Checkpoint           — thread-safe FIFO, backpressure modes
  CheckpointSnapshot   — immutable state snapshot for policy decisions
  Stage                — runtime wrapper: checkpoint + component + concurrency
  ExecutorPool         — platform + virtual thread management
  DependencyGraph      — topological sort, cycle detection
  DefaultSchedulingPolicy — depth-first + oldest-wait
  RoundRobinPolicy     — fair cycling
  WeightedFairSharePolicy — proportional allocation
  ShortestProcessingTimePolicy — latency-minimizing
  LowestErrorRatePolicy — quality-biased
  CompositePolicy       — chains multiple policies
  PipelineResult        — final statistics

USER IMPLEMENTS (the only things not provided):
  Stage components      — the actual processing logic (Tokenizer, NerAnnotator, etc.)
  Custom SchedulingPolicy — if the defaults don't fit
  Custom TelemetrySink   — if file/console isn't enough
```

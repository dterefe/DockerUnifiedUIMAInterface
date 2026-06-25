# DUUI Architecture — Redesigned (Trait-Driven Entity Management System)

> **Generated**: 2026-06-25 | **Redesign**: Trait-driven composition with shallow entity inheritance
> **Modules**: `duui-base` (foundational traits, entities, managers) | `duui-core` (orchestration & pipeline)
> **Total**: ~300 Java files (redesigned from 293)

---

## Table of Contents

1. [Redesign Philosophy](#redesign-philosophy)
2. [Architectural Overview](#architectural-overview)
3. [Foundation: Traits](#foundation-traits)
4. [Foundation: DUUIEntity&lt;T&gt;](#foundation-duuientityt)
5. [Foundation: DUUIManager&lt;T&gt;](#foundation-duuimanagert)
6. [Foundation: DUUIDescription & DUUIConfiguration](#foundation-duuidescription--duuiconfiguration)
7. [Trait Catalogue](#trait-catalogue)
   - [DUUIBehaviour](#duuibehaviour)
   - [DUUIProtocol](#duuiprotocol)
   - [DUUISpecification](#duuispecification)
   - [DUUIPolicy](#duuipolicy)
   - [DUUIStrategy](#duuistrategy)
   - [DUUILifecycle](#duuilifecycle)
   - [DUUIConcurrency](#duuiconcurrency)
   - [DUUICapability](#duuicapability)
8. [Entity Catalogue](#entity-catalogue)
   - [Core Entities](#core-entities)
   - [Actor Entities](#actor-entities)
   - [Resource Entities](#resource-entities)
   - [Service Entities](#service-entities)
   - [Client Entities](#client-entities)
9. [Module: duui-core (Redesigned)](#module-duui-core-redesigned)
   - [Component Model](#component-model)
   - [Driver System](#driver-system)
   - [Execution Engine](#execution-engine)
   - [Scheduler](#scheduler)
   - [Workflow & Pipeline](#workflow--pipeline)
10. [Cross-Cutting Architecture](#cross-cutting-architecture)
11. [Key Design Patterns](#key-design-patterns)
12. [Migration from Legacy](#migration-from-legacy)

---

## Redesign Philosophy

### The Problem with the Legacy EMS

The original DUUI Entity Management System used **deep interface inheritance**:

```
DUUIEntity
├── DUUIActor
├── DUUIContext
├── DUUIResource
├── DUUIService
│   ├── DUUIClient
│   │   ├── DUUIFileSystemClient
│   │   ├── DUUIVirtualizationClient
│   │   └── ...
│   └── DUUIRuntimeService
│       ├── DUUIEntityManager
│       ├── DUUIProfileManager
│       └── ...
```

This created rigid taxonomies where adding a new capability required either:
- Adding to an existing interface (breaking all implementations), or
- Creating a new branch in the hierarchy (combinatorial explosion)

### The Trait-Driven Solution

**Everything is a trait.** Entities are identified by their **composition of traits**, not their position in a class hierarchy.

```
DUUIEntity<Identity & Addressable & Loggable & ...>
```

A `DUUIEntity` is a shallow generic container `DUUIEntity<T extends DUUITrait>` whose type parameter `T` is an **intersection type** of traits. The entity's capabilities, protocols, policies, and lifecycle are ALL defined by the traits it composes.

### Core Principles

| Principle | Description |
|-----------|-------------|
| **Trait-First** | Services, resources, endpoints, annotators, clients — all are traits, not classes |
| **Shallow Inheritance** | Only two concrete base classes: `DUUIEntity<T>` and `DUUIManager<T>` |
| **Composition over Inheritance** | Behaviours composed via intersection types `A & B & C` |
| **Framework Boilerplate Centrally** | Inheritance enforces telemetry, identity, lifecycle — all managed in the base classes |
| **Type-Parameter-Driven State** | Concurrency model, lifecycle state modeled through type parameters |
| **Description-Driven Configuration** | `DUUIDescription` serialized form generates `DUUIBuilder` + `DUUIConfiguration` |

---

## Architectural Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│ duui-core (orchestration & pipeline)                                  │
│                                                                      │
│ DUUIPipeline ──── orchestrates ──── DUUIStage/Sequence/Fork/Join     │
│ DUUIComponent<Pooled> ── managed replicas ── DUUIAnnotator<Slot>     │
│ DUUIScheduler<Priority> ── checkpoint selection                      │
│ DUUIExecutor ──────── task dispatch ──────── DUUITask<R>             │
│ DUUIV1Driver<Docker> ─── creates ─── DUUIAnnotator<V1Protocol>       │
├──────────────────────────────────────────────────────────────────────┤
│ duui-base (trait-driven foundation)                                   │
│                                                                      │
│ DUUITrait ◄── DUUIBehaviour, DUUIProtocol, DUUISpecification,        │
│               DUUIPolicy, DUUIStrategy, DUUILifecycle,                │
│               DUUIConcurrency, DUUICapability                        │
│                                                                      │
│ DUUIEntity<T extends DUUITrait>  ─── all domain objects              │
│   └── DUUIManager<T extends DUUITrait>  ─── runtime singletons       │
│                                                                      │
│ DUUIDescription ──► DUUIBuilder ──► DUUIConfiguration                │
│                                                                      │
│ GID, DUUIProfile, DUUITimeline, DUUITelemetryService                 │
│ DUUIPipe, DUUIBridge, DUUIPool, DUUICheckpoint                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Foundation: Traits

### `DUUITrait` (Root Interface)

**Package**: `org.texttechnologylab.duui.base.trait`

The root of ALL behavioural contracts. Every capability, protocol, policy, strategy, lifecycle model, and concurrency model is a `DUUITrait`.

```java
public interface DUUITrait {
    /** Unique trait identifier for runtime discovery. */
    String traitId();

    /** Human-readable label for this trait. */
    default String label() { return traitId(); }

    /** Priority for conflict resolution when multiple traits apply. */
    default int priority() { return 0; }
}
```

### Trait Hierarchy

```
DUUITrait (root — everything is a trait)
│
├── DUUIBehaviour        — "how an entity acts"
│   ├── Loggable         — emits log events
│   ├── Measurable       — emits metrics
│   ├── Traceable        — participates in distributed tracing
│   ├── Observable       — exposes health indicators
│   ├── Profilable       — can be profiled (JFR, JMX)
│   ├── Versionable      — has a version
│   └── Attributable     — carries key-value attributes
│
├── DUUIProtocol         — "how an entity communicates"
│   ├── HTTP             — speaks HTTP (REST)
│   ├── gRPC             — speaks gRPC
│   ├── UIMA             — speaks UIMA CAS protocol
│   ├── Streaming        — byte-stream oriented
│   ├── MessagePack      — MessagePack serialization
│   └── Lua              — Lua-driven serialization
│
├── DUUISpecification    — "what an entity declares about itself"
│   ├── Identity         — has a canonical identity
│   ├── Address          — is network-addressable
│   ├── Resource         — consumes/provides resources
│   ├── Capability       — declares what it can do
│   ├── Contract         — declares input/output types
│   └── MetricSpec       — declares emitted metrics
│
├── DUUIPolicy           — "what rules govern an entity"
│   ├── FailurePolicy    — how failures are handled
│   ├── RetryPolicy      — retry configuration
│   ├── QuarantinePolicy — when to isolate
│   ├── RateLimitPolicy  — throttling rules
│   ├── TimeoutPolicy    — timeout configuration
│   └── AdmissionPolicy  — checkpoint admission
│
├── DUUIStrategy         — "how an entity decides"
│   ├── SchedulingStrategy — checkpoint/task selection
│   ├── RoutingStrategy    — request routing
│   ├── LoadBalancing      — replica selection
│   ├── BackoffStrategy    — retry backoff algorithm
│   └── CachingStrategy    — cache eviction
│
├── DUUILifecycle        — "what states an entity traverses"
│   ├── Creatable        — can be created
│   ├── Startable        — can be started
│   ├── Pausable         — can be paused/resumed
│   ├── Stoppable        — can be stopped
│   ├── Restartable      — can be restarted
│   └── Disposable       — can be disposed/closed
│
└── DUUIConcurrency      — "how an entity handles concurrency"
    ├── SingleThreaded   — one thread at a time
    ├── ThreadBound      — bound to a specific thread
    ├── Slot             — single concurrent operation
    ├── Pooled           — fixed-size concurrency pool
    ├── Unbounded        — unlimited concurrency
    ├── Virtual          — virtual-thread-per-task
    └── Platform         — platform-thread-per-task
```

### Trait Composition via Intersection Types

Java's intersection types allow entities to declare precisely which traits they compose:

```java
// A V1 annotator that is pooled, HTTP-speaking, retryable, and measurable
DUUIEntity<UIMA & HTTP & Pooled & RetryPolicy & Measurable>

// A filesystem client that is addressable, rate-limited, and observable
DUUIManager<Address & FileSystem & RateLimitPolicy & Observable>

// A checkpoint that is admission-controlled with backpressure strategy
DUUIEntity<Queue & AdmissionPolicy & BackoffStrategy & Measurable>
```

---

## Foundation: DUUIEntity&lt;T&gt;

**Package**: `org.texttechnologylab.duui.base.entity`

### `DUUIEntity<T extends DUUITrait>` (Abstract Class)

The **single base class** for ALL domain objects. Provides centralized framework boilerplate that every entity inherits:

```java
public abstract class DUUIEntity<T extends DUUITrait> implements AutoCloseable {

    // ── Identity (framework-managed) ──────────────────────────
    protected final GID gid;                    // immutable, assigned at construction
    public final GID gid() { return gid; }

    // ── Traits (composed capabilities) ────────────────────────
    protected final DUUITraits traits;          // immutable set of composed traits
    public final DUUITraits traits() { return traits; }
    public final <U extends DUUITrait> boolean has(Class<U> traitType) { ... }
    public final <U extends DUUITrait> Optional<U> get(Class<U> traitType) { ... }

    // ── Profile (framework-managed, derived from traits) ──────
    public final DUUIProfile profile() { ... }  // aggregates trait specifications

    // ── Timeline (framework-managed) ──────────────────────────
    protected final DUUITimeline timeline;
    public final DUUITimeline timeline() { return timeline; }

    // ── Telemetry (framework-managed) ─────────────────────────
    public final DUUILogger logger() { ... }    // bound to this entity
    public final DUUIResourceContext context() { ... }

    // ── Relationships (framework-managed) ─────────────────────
    public final Stream<DUUIRelationship> outgoing() { ... }
    public final Stream<DUUIRelationship> incoming() { ... }

    // ── Description (trait-driven serialized form) ────────────
    public abstract DUUIDescription describe();

    // ── Builder (generated from description) ──────────────────
    public abstract DUUIBuilder<? extends DUUIEntity<T>> builder();

    // ── Configuration (generated from description) ────────────
    public abstract DUUIConfiguration<T> configuration();
}
```

**Key Design Points**:
- `T` captures the entity's trait composition — this IS the entity's type identity
- All boilerplate (GID, timeline, telemetry, relationships) is managed by the framework in the base class
- `describe()`, `builder()`, `configuration()` are abstract — each concrete entity implements them
- `has(Class)` and `get(Class)` provide runtime trait introspection

### Entity Identity via Trait Composition

Two entities are "the same kind of thing" if they compose the same traits, NOT if they share a class:

```java
// These are semantically equivalent — both are pooled HTTP UIMA annotators
var a = new DUUIAnnotator<UIMA & HTTP & Pooled>(...);
var b = new DUUIAnnotator<UIMA & HTTP & Pooled>(...);
// a.traits().equals(b.traits()) == true
```

---

## Foundation: DUUIManager&lt;T&gt;

**Package**: `org.texttechnologylab.duui.base.entity`

### `DUUIManager<T extends DUUITrait> extends DUUIEntity<T>` (Abstract Class)

For **runtime singleton entities** — clients, services, registries, schedulers. A `DUUIManager` is an entity of which only **one instance** exists per runtime.

```java
public abstract class DUUIManager<T extends DUUITrait> extends DUUIEntity<T> {

    // ── Singleton enforcement ─────────────────────────────────
    private static final ConcurrentHashMap<Class<?>, DUUIManager<?>> INSTANCES = ...;

    protected DUUIManager(GID gid, DUUITraits traits) {
        super(gid, traits);
        var previous = INSTANCES.putIfAbsent(getClass(), this);
        if (previous != null) {
            throw new DUUIContractException("DUUIManager " + getClass() + " already exists");
        }
    }

    public static <M extends DUUIManager<?>> M instance(Class<M> managerClass) { ... }

    // ── Runtime lifecycle ─────────────────────────────────────
    public abstract void initialize(DUUIRuntime runtime);
    public abstract void shutdown();

    // ── Health ────────────────────────────────────────────────
    public abstract DUUIResourceUseSnapshot health();
}
```

**What is a Manager?**
- `DUUITelemetryService` → `DUUIManager<Observable & Measurable>`
- `DUUIEntityRegistry` → `DUUIManager<Registry & Indexed>`
- `DUUIHttpClient` → `DUUIManager<HTTP & RateLimitPolicy & Observable>`
- `DUUIDockerClient` → `DUUIManager<ContainerRuntime & ImageManagement & Observable>`
- `DUUIScheduler` → `DUUIManager<SchedulingStrategy & Measurable>`
- `DUUIExecutorService` → `DUUIManager<Dispatch & Pooled & Measurable>`

---

## Foundation: DUUIDescription & DUUIConfiguration

### `DUUIDescription` (Record)

**Package**: `org.texttechnologylab.duui.base.description`

The **serialized representation** of an entity's trait composition and configuration. This is the bridge between declarative configuration (YAML, JSON) and runtime entities.

```java
public record DUUIDescription(
    String entityType,              // FQCN of the entity class (e.g., "DUUIAnnotator")
    Set<String> traitIds,           // trait identifiers composing this entity
    Map<String, Object> attributes, // key-value configuration
    List<DUUIDescription> children, // sub-entities (e.g., replicas in a component)
    DUUIVersion version             // schema version
) {
    /** Generate a builder pre-populated from this description. */
    public DUUIBuilder<?> toBuilder() { ... }

    /** Validate that all traitIds are resolvable. */
    public DUUIValidationResult validate() { ... }
}
```

### `DUUIBuilder<T extends DUUIEntity<?>>` (Interface)

Generated from a `DUUIDescription`. Provides a fluent builder for constructing entities:

```java
public interface DUUIBuilder<T extends DUUIEntity<?>> {
    DUUIBuilder<T> withTrait(Class<? extends DUUITrait> trait);
    DUUIBuilder<T> withAttribute(String key, Object value);
    DUUIBuilder<T> withChild(DUUIDescription child);
    T build(DUUIRuntime runtime);
}
```

### `DUUIConfiguration<T extends DUUITrait>` (Record)

Immutable, validated configuration snapshot generated from a `DUUIDescription`:

```java
public record DUUIConfiguration<T extends DUUITrait>(
    Class<? extends DUUIEntity<T>> entityClass,
    DUUITraits traits,
    Map<String, Object> parameters,
    List<DUUIConfiguration<?>> children
) {
    public <U extends DUUITrait> boolean hasTrait(Class<U> traitType) { ... }
    public <U extends DUUITrait> U getTrait(Class<U> traitType) { ... }
}
```

### The Description → Builder → Configuration Pipeline

```
YAML/JSON ──parse──► DUUIDescription ──validate──► DUUIConfiguration<T>
                           │                              │
                           ▼                              ▼
                     DUUIBuilder<T> ──build──► DUUIEntity<T>
```

---

## Trait Catalogue

### DUUIBehaviour

**Package**: `org.texttechnologylab.duui.base.trait.behaviour`

"**How an entity acts**" — behavioural traits that entities can exhibit.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `Loggable` | `extends DUUIBehaviour` | Entity emits structured log events |
| `Measurable` | `extends DUUIBehaviour` | Entity emits metrics |
| `Traceable` | `extends DUUIBehaviour` | Entity participates in distributed tracing |
| `Observable` | `extends DUUIBehaviour` | Entity exposes health indicators |
| `Profilable` | `extends DUUIBehaviour` | Entity can be profiled (JFR, JMX, async) |
| `Versionable` | `extends DUUIBehaviour` | Entity carries a semantic version |
| `Attributable` | `extends DUUIBehaviour` | Entity carries key-value attributes |
| `Indexable` | `extends DUUIBehaviour` | Entity participates in indexed lookups |
| `Cacheable` | `extends DUUIBehaviour` | Entity results are cacheable |

### DUUIProtocol

**Package**: `org.texttechnologylab.duui.base.trait.protocol`

"**How an entity communicates**" — protocol traits defining serialization, transport, and API contracts.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `HTTP` | `extends DUUIProtocol` | REST/HTTP communication |
| `gRPC` | `extends DUUIProtocol` | gRPC communication |
| `UIMA` | `extends DUUIProtocol` | UIMA CAS protocol (V1/V2) |
| `Streaming` | `extends DUUIProtocol` | Byte-stream oriented I/O |
| `MessagePack` | `extends DUUIProtocol` | MessagePack binary serialization |
| `Lua` | `extends DUUIProtocol` | Lua-driven serialization/deserialization |
| `XMI` | `extends DUUIProtocol` | UIMA XMI serialization |
| `JSON` | `extends DUUIProtocol` | JSON serialization |
| `FileSystem` | `extends DUUIProtocol` | File-system access protocol |
| `ContainerRuntime` | `extends DUUIProtocol` | Container lifecycle (Docker/Podman/K8s API) |
| `WebSocket` | `extends DUUIProtocol` | WebSocket bidirectional communication |

### DUUISpecification

**Package**: `org.texttechnologylab.duui.base.trait.specification`

"**What an entity declares about itself**" — typed contracts that entities publish.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `Identity` | `extends DUUISpecification` | Canonical identity declaration |
| `Address` | `extends DUUISpecification` | Network address declaration (scheme + host + port) |
| `Resource` | `extends DUUISpecification` | Resource consumption/provision declaration |
| `Capability` | `extends DUUISpecification` | Declared capabilities |
| `Contract` | `extends DUUISpecification` | Input/output type contract |
| `MetricSpec` | `extends DUUISpecification` | Emitted metric declarations |
| `Endpoint` | `extends DUUISpecification` | Service endpoint declaration |
| `Image` | `extends DUUISpecification` | Container image reference |
| `Scale` | `extends DUUISpecification` | Replica count and concurrency limits |

### DUUIPolicy

**Package**: `org.texttechnologylab.duui.base.trait.policy`

"**What rules govern an entity**" — decision rules for failure handling, rate limiting, etc.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `FailurePolicy` | `extends DUUIPolicy` | How failures are categorized and handled |
| `RetryPolicy` | `extends DUUIPolicy` | Retry configuration (max attempts, backoff) |
| `QuarantinePolicy` | `extends DUUIPolicy` | When to isolate a failing replica |
| `RateLimitPolicy` | `extends DUUIPolicy` | Throttling and rate limiting rules |
| `TimeoutPolicy` | `extends DUUIPolicy` | Operation timeout configuration |
| `AdmissionPolicy` | `extends DUUIPolicy` | Checkpoint admission control |
| `CircuitBreakerPolicy` | `extends DUUIPolicy` | Circuit breaker configuration |
| `ConcurrencyPolicy` | `extends DUUIPolicy` | Max concurrency, queue depth limits |

### DUUIStrategy

**Package**: `org.texttechnologylab.duui.base.trait.strategy`

"**How an entity decides**" — pluggable algorithms for scheduling, routing, load balancing.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `SchedulingStrategy` | `extends DUUIStrategy` | Checkpoint and task selection algorithm |
| `RoutingStrategy` | `extends DUUIStrategy` | Request routing to replicas |
| `LoadBalancing` | `extends DUUIStrategy` | Replica selection for load distribution |
| `BackoffStrategy` | `extends DUUIStrategy` | Retry backoff algorithm (fixed, exponential, jitter) |
| `CachingStrategy` | `extends DUUIStrategy` | Cache eviction and population strategy |
| `DiscoveryStrategy` | `extends DUUIStrategy` | File/resource discovery algorithm |

### DUUILifecycle

**Package**: `org.texttechnologylab.duui.base.trait.lifecycle`

"**What states an entity traverses**" — lifecycle state machines as traits.

| Trait | Interface | States |
|-------|-----------|--------|
| `Creatable` | `extends DUUILifecycle` | CREATED → INITIALIZING → READY |
| `Startable` | `extends DUUILifecycle` | READY → STARTING → RUNNING |
| `Pausable` | `extends DUUILifecycle` | RUNNING → PAUSING → PAUSED → RESUMING → RUNNING |
| `Stoppable` | `extends DUUILifecycle` | RUNNING → STOPPING → STOPPED |
| `Restartable` | `extends DUUILifecycle` | STOPPED → RESTARTING → RUNNING |
| `Disposable` | `extends DUUILifecycle` | STOPPED → DISPOSING → DISPOSED |
| `Failable` | `extends DUUILifecycle` | RUNNING → FAILING → FAILED |
| `Retryable` | `extends DUUILifecycle` | FAILED → RETRYING → RUNNING |

### DUUIConcurrency

**Package**: `org.texttechnologylab.duui.base.trait.concurrency`

"**How an entity handles concurrency**" — threading and parallelism models as traits.

| Trait | Interface | Purpose |
|-------|-----------|---------|
| `SingleThreaded` | `extends DUUIConcurrency` | Only one thread may interact at a time |
| `ThreadBound` | `extends DUUIConcurrency` | Bound to a specific thread (thread-local) |
| `Slot` | `extends DUUIConcurrency` | Single concurrent operation slot (mutex) |
| `Pooled` | `extends DUUIConcurrency` | Fixed-size concurrency pool |
| `Unbounded` | `extends DUUIConcurrency` | Unlimited concurrent operations |
| `Virtual` | `extends DUUIConcurrency` | Virtual-thread-per-task |
| `Platform` | `extends DUUIConcurrency` | Platform-thread-per-task |
| `Serialized` | `extends DUUIConcurrency` | Operations serialized in order |
| `Partitioned` | `extends DUUIConcurrency` | Operations partitioned by key |

---

## Entity Catalogue

### Core Entities

All concrete entities extend `DUUIEntity<T>` with specific trait compositions.

#### `DUUIAnnotator<C extends DUUIConcurrency>`

```java
public final class DUUIAnnotator<C extends DUUIConcurrency>
    extends DUUIEntity<UIMA & HTTP & C & Measurable & RetryPolicy> { ... }
```

**The type parameter `C` models the concurrency model and state/lifecycle:**
- `DUUIAnnotator<Slot>` — single-operation annotator (thread-bound)
- `DUUIAnnotator<Pooled>` — pooled annotator with concurrency slots
- `DUUIAnnotator<Virtual>` — virtual-thread-per-request annotator

The entity's state transitions are derived from `C`'s lifecycle — a `Slot` annotator has different state transitions than a `Pooled` one.

#### `DUUIComponent<C extends DUUIConcurrency>`

```java
public final class DUUIComponent<C extends DUUIConcurrency>
    extends DUUIEntity<Pooled & Measurable & FailurePolicy & QuarantinePolicy> {
    // Manages a pool of DUUIAnnotator<C> replicas
    private final DUUIPool<DUUIAnnotator<C>> replicas;
}
```

#### `DUUIWorker<C extends DUUIConcurrency>`

```java
public final class DUUIWorker<C extends DUUIConcurrency>
    extends DUUIEntity<C & Measurable & Traceable & Startable & Stoppable> { ... }
```

- `DUUIWorker<ThreadBound>` — thread-local worker (legacy `DUUIWorker`)
- `DUUIWorker<Virtual>` — virtual-thread worker
- `DUUIWorker<Platform>` — platform-thread worker

#### `DUUINode`

```java
public final class DUUINode
    extends DUUIEntity<Identity & Address & Observable & Measurable> { ... }
```

Machine identity with STABLE/EPOCH/HEURISTIC tiers (unchanged from legacy except trait-based).

#### `DUUIStage<C extends DUUIConcurrency>`

```java
public sealed interface DUUIStage<C extends DUUIConcurrency>
    permits DUUIStage.Source, DUUIStage.Processor, DUUIStage.Fork,
            DUUIStage.Join, DUUIStage.Target {

    record Source<C>(...) extends DUUIEntity<C & Discoverable & Measurable>
        implements DUUIStage<C> {}
    record Processor<C>(...) extends DUUIEntity<C & Measurable & Contract>
        implements DUUIStage<C> {}
    record Fork<C>(...) extends DUUIEntity<C & Partitioned & Measurable>
        implements DUUIStage<C> {}
    record Join<C>(...) extends DUUIEntity<C & Measurable>
        implements DUUIStage<C> {}
    record Target<C>(...) extends DUUIEntity<C & Measurable & Stoppable>
        implements DUUIStage<C> {}
}
```

### Resource Entities

#### `DUUIPipe`

```java
public final class DUUIPipe
    extends DUUIEntity<Streaming & Measurable & Disposable> { ... }
```

Async byte channel with lifecycle management.

#### `DUUIBridge`

```java
public final class DUUIBridge
    extends DUUIEntity<Streaming & Measurable & Disposable> { ... }
```

Binds two pipes with cascading cancellation.

#### `DUUIPool<T extends DUUIEntity<?>>`

```java
public final class DUUIPool<T extends DUUIEntity<?>>
    extends DUUIEntity<Pooled & Measurable & Observable> {
    // Concurrency controlled by Pooled trait
}
```

#### `DUUICheckpoint<T>`

```java
public final class DUUICheckpoint<T>
    extends DUUIEntity<Queue & AdmissionPolicy & BackoffStrategy & Measurable> { ... }
```

Admission-controlled task queue. The admission policy and backoff strategy are trait-driven (pluggable).

#### `DUUILease<T>`

```java
public final class DUUILease<T>
    extends DUUIEntity<Slot & TimeoutPolicy & Disposable> implements AutoCloseable { ... }
```

Time-limited resource borrow. `Slot` trait enforces single-holder semantics.

### Service Entities (DUUIManager)

#### `DUUITelemetryService`

```java
public final class DUUITelemetryService
    extends DUUIManager<Observable & Measurable & Traceable> { ... }
```

Central orchestrator for logs, metrics, traces. Singleton managed.

#### `DUUIEntityRegistry`

```java
public final class DUUIEntityRegistry
    extends DUUIManager<Indexable & Observable> {
    private final ConcurrentHashMap<GID, DUUIEntity<?>> entities = ...;
}
```

#### `DUUIHttpClient`

```java
public final class DUUIHttpClient
    extends DUUIManager<HTTP & RateLimitPolicy & Observable & TimeoutPolicy> { ... }
```

#### `DUUIDockerClient`

```java
public final class DUUIDockerClient
    extends DUUIManager<ContainerRuntime & ImageManagement & Observable> { ... }
```

#### `DUUIFileSystemClient`

```java
public final class DUUIFileSystemClient
    extends DUUIManager<FileSystem & Address & Observable> { ... }
```

#### `DUUIScheduler<ST extends SchedulingStrategy>`

```java
public final class DUUIScheduler<ST extends SchedulingStrategy>
    extends DUUIManager<ST & Measurable & Observable> { ... }
```

- `DUUIScheduler<Priority>` — priority-based scheduling
- `DUUIScheduler<FIFO>` — first-in-first-out
- `DUUIScheduler<FairShare>` — fair-share scheduling

---

## Module: duui-core (Redesigned)

### Component Model

**Package**: `org.texttechnologylab.duui.core.component`

#### Communication Layers (Protocol Traits)

| Entity | Trait Composition | Purpose |
|--------|-------------------|---------|
| `DUUICommunicationLayer` | `DUUIEntity<UIMA & Serialization>` | Serialize/deserialize CAS ↔ wire |
| `DUUIXmiCommunicationLayer` | `DUUIEntity<XMI & UIMA>` | XMI-based serialization |
| `DUUILuaCommunicationLayer` | `DUUIEntity<Lua & UIMA>` | Lua-driven serialization |

#### Annotator Hierarchy

| Entity | Trait Composition | Purpose |
|--------|-------------------|---------|
| `DUUIAnnotator<C>` | `DUUIEntity<UIMA & HTTP & C & Measurable & RetryPolicy>` | Base annotator with concurrency model `C` |
| `DUUIV1Annotator<C>` | `DUUIAnnotator<C> & V1Protocol` | HTTP-based DUUI V1 annotator |
| `DUUIAnalysisEngine` | `DUUIEntity<UIMA & InProcess & Measurable>` | In-process UIMA AnalysisEngine wrapper |

#### `DUUIComponent<C extends DUUIConcurrency>`

```java
public final class DUUIComponent<C extends DUUIConcurrency>
    extends DUUIEntity<Pooled & Measurable & FailurePolicy & QuarantinePolicy & RetryPolicy> {

    private final DUUIPool<DUUIAnnotator<C>> replicas;
    private final DUUIFailurePolicy failurePolicy;   // from FailurePolicy trait
    private final DUUIRetryPolicy retryPolicy;       // from RetryPolicy trait
    private final DUUIQuarantinePolicy quarantinePolicy; // from QuarantinePolicy trait

    public DUUIFlow<DUUIArtifact<JCas>> process(DUUIArtifact<JCas> artifact) {
        // Acquire lease (governed by Pooled trait)
        // Dispatch to annotator (governed by C concurrency trait)
        // Handle failures (governed by FailurePolicy + RetryPolicy)
        // Quarantine on threshold (governed by QuarantinePolicy)
    }
}
```

### Driver System

**Package**: `org.texttechnologylab.duui.core.driver`

Drivers convert `DUUIDescription` → `DUUIComponent<C>` instances. Each driver is itself an entity with its own trait composition.

| Driver | Trait Composition | Purpose |
|--------|-------------------|---------|
| `DUUIV1Driver` | `DUUIEntity<V1Protocol & Measurable>` | Base: creates V1 annotators from descriptions |
| `DUUIVirtualizationDriver` | `DUUIV1Driver & ContainerRuntime` | Adds container lifecycle |
| `DUUIRemoteDriver` | `DUUIV1Driver & Remote` | Pre-existing remote endpoints |
| `DUUIDockerDriver` | `DUUIVirtualizationDriver & Docker` | Docker-specific |
| `DUUIPodmanDriver` | `DUUIVirtualizationDriver & Podman` | Podman-specific |
| `DUUIKubernetesDriver` | `DUUIVirtualizationDriver & Kubernetes` | Kubernetes-specific |
| `DUUISwarmDriver` | `DUUIVirtualizationDriver & Swarm` | Docker Swarm-specific |
| `DUUIUIMADriver` | `DUUIEntity<UIMA & InProcess & Measurable>` | In-process UIMA engines |

### Execution Engine

**Package**: `org.texttechnologylab.duui.core.execution`

| Entity | Trait Composition | Purpose |
|--------|-------------------|---------|
| `DUUITask<R>` | `DUUIEntity<Measurable & Traceable & Retryable>` | Unit of work with state machine |
| `DUUIExecutorService` | `DUUIManager<Dispatch & Pooled & Virtual & Measurable>` | Task dispatcher (singleton) |
| `DUUIExecutionLane` | `DUUIEntity<Platform | Virtual>` | Execution lane selection |
| `DUUITaskState` | enum (mapped to `DUUILifecycle` traits) | Task lifecycle states |

### Scheduler

**Package**: `org.texttechnologylab.duui.core.scheduler`

| Entity | Trait Composition | Purpose |
|--------|-------------------|---------|
| `DUUIScheduler<ST>` | `DUUIManager<ST & Measurable & Observable>` | Checkpoint scheduler with pluggable `SchedulingStrategy` |
| `DUUISchedulingDecision` | record | Decision: ADVANCE/WAIT with metadata |

### Workflow & Pipeline

**Package**: `org.texttechnologylab.duui.core.workflow`

#### `DUUIPipeline`

```java
public final class DUUIPipeline
    extends DUUIEntity<Pooled & Measurable & Observable & Startable & Stoppable>
    implements AutoCloseable {

    // Stages with their concurrency models
    private final List<DUUIStage<? extends DUUIConcurrency>> stages;

    // Builder (generated from DUUIDescription)
    public static PipelineBuilder fromDescription(DUUIDescription desc) { ... }

    // Inner builder
    public static final class PipelineBuilder
        implements DUUIBuilder<DUUIPipeline> { ... }
}
```

#### Builder Pattern (Description-Driven)

```java
// YAML description → DUUIDescription → DUUIBuilder → DUUIPipeline
var description = DUUIDescription.parse(yamlString);
var pipeline = DUUIPipeline.fromDescription(description)
    .withComponent("tokenizer",
        DUUIComponentDescription.builder()
            .withTrait(HTTP)
            .withTrait(UIMA)
            .withTrait(Pooled)
            .withTrait(RetryPolicy)
            .withConcurrency(Slot.class)
            .withEndpoint("http://tokenizer:8080")
            .build())
    .build(runtime);
```

---

## Cross-Cutting Architecture

### Trait Composition at Every Layer

```
┌──── duui-core ───────────────────────────────────────────────────┐
│                                                                    │
│ DUUIPipeline<Pooled & Measurable & Observable>                     │
│   ├── DUUIStage.Processor<Partitioned>                             │
│   │     └── DUUIComponent<Pooled & FailurePolicy & RetryPolicy>    │
│   │           └── DUUIPool<DUUIAnnotator<Slot>>                    │
│   │                 └── DUUIAnnotator<Slot & UIMA & HTTP>          │
│   ├── DUUIStage.Fork<Virtual>                                      │
│   │     └── DUUIComponent<Virtual & Measurable>                    │
│   └── DUUIStage.Target<SingleThreaded & Stoppable>                 │
│                                                                    │
│ DUUIScheduler<Priority & Measurable & Observable>                  │
│   └── DUUICheckpoint<Queue & AdmissionPolicy & BackoffStrategy>    │
│                                                                    │
│ DUUIExecutorService<Dispatch & Pooled & Virtual & Measurable>       │
│   └── DUUITask<R> <Measurable & Traceable & Retryable>             │
│                                                                    │
├────────────────────────────────────────────────────────────────────┤
│ ┌──── duui-base ───────────────────────────────────────────────┐  │
│ │                                                                │  │
│ │ DUUITrait ◄── 8 trait families, 60+ individual traits         │  │
│ │ DUUIEntity<T extends DUUITrait>  ─── all domain objects       │  │
│ │   └── DUUIManager<T extends DUUITrait>  ─── singletons        │  │
│ │                                                                │  │
│ │ DUUIDescription ──► DUUIBuilder ──► DUUIConfiguration        │  │
│ │                                                                │  │
│ │ GID, DUUIProfile, DUUITimeline, DUUITelemetryService          │  │
│ │ DUUIPipe, DUUIBridge, DUUIPool, DUUICheckpoint, DUUILease     │  │
│ └────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

### Layered Dependency (Trait-First)

All dependencies are at the trait level. An entity depends on traits, not on other entity classes:

```
DUUIComponent<Pooled> depends on DUUIAnnotator<Slot>
  └── BUT: expressed as trait dependency: Pooled depends on Slot
  └── DUUIComponent doesn't import DUUIAnnotator — it imports DUUISlot trait

DUUIScheduler<Priority> depends on DUUICheckpoint<?>
  └── BUT: expressed as trait dependency: Priority SchedulingStrategy
           depends on Queue & AdmissionPolicy
```

---

## Key Design Patterns

### 1. Trait as Universal Contract
Every capability, protocol, policy, strategy, lifecycle model, and concurrency model is a `DUUITrait`. There is no other way to declare what an entity is or does.

### 2. Entity as Trait Container
`DUUIEntity<T extends DUUITrait>` is a shallow container. Its identity is `T` — the set of traits it composes. The class hierarchy is only two levels deep (`DUUIEntity` → `DUUIManager`).

### 3. Framework Boilerplate Centrally
All identity (GID), profiling, telemetry, relationship tracking, and timeline management is implemented ONCE in `DUUIEntity<T>` and inherited by everything. No entity class reimplements these.

### 4. Type-Parameter-Driven Concurrency
`DUUIAnnotator<Slot>` vs `DUUIAnnotator<Pooled>` — the concurrency model AND its lifecycle states are encoded in the type parameter `C extends DUUIConcurrency`. The same entity class behaves differently based on `C`.

### 5. Description → Builder → Configuration Pipeline
`DUUIDescription` (serialized) → `DUUIBuilder` (fluent construction) → `DUUIConfiguration` (validated snapshot). This enables YAML/JSON-driven entity construction without reflection.

### 6. Manager Singleton Pattern
`DUUIManager<T>` enforces runtime singleton semantics for services, clients, registries — anything that should have exactly one instance per runtime.

### 7. Trait Introspection at Runtime
`entity.has(RetryPolicy.class)` and `entity.get(RetryPolicy.class)` enable runtime decision-making based on trait composition without `instanceof` chains.

### 8. Immutable Trait Sets
`DUUITraits` is an immutable, set-backed collection. Two entities with the same trait set are semantically equivalent regardless of their concrete class.

### 9. Centralized Telemetry via Traits
`Measurable`, `Observable`, `Traceable`, `Loggable` traits are recognized by `DUUITelemetryService` which automatically wires instrumentation for any entity that composes them.

### 10. Provider Pattern via Protocol Traits
`FileSystem`, `ContainerRuntime`, `HTTP` are protocol traits. Backends (S3, Docker, Podman) implement the protocol trait, not a class hierarchy. New providers add trait implementations, not subclasses.

---

## Migration from Legacy

### Key Changes

| Legacy | Redesigned |
|--------|------------|
| `DUUIEntity` (interface) | `DUUIEntity<T>` (abstract class) |
| `DUUIActor` (interface) | `DUUIEntity<Agent & ...>` with `Agent` trait |
| `DUUIService` (interface) | `DUUIManager<...>` or `DUUIEntity<Service & ...>` |
| `DUUIResource` (interface) | `DUUIEntity<Resource & ...>` with `Resource` trait |
| `DUUIClient` (interface) | `DUUIManager<...>` with protocol traits |
| `DUUIWorker` (final class) | `DUUIWorker<C extends DUUIConcurrency>` with `C` modelling thread model |
| `DUUIAnnotator` (base class) | `DUUIAnnotator<C extends DUUIConcurrency>` |
| `DUUIComponent` (final class) | `DUUIComponent<C extends DUUIConcurrency>` |
| `DUUIPool<Permit>` | `DUUIPool<DUUIEntity<?>>` with `Pooled` trait |
| `DUUICheckpoint` admission | `DUUICheckpoint<AdmissionPolicy & BackoffStrategy>` |
| Deep interface hierarchy | Shallow 2-level inheritance + trait composition |
| `@Phase` AOP lifecycle | Trait-based lifecycle (`Startable`, `Stoppable`, etc.) |
| `DUUISpecification<T>` interface | `DUUISpecification` trait family |
| `DUUISpecifier<T>` interface | Merged into `DUUISpecification` trait |
| `DUUITrait` (marker interface) | `DUUITrait` (root of all behaviour contracts) |

### Migration Strategy
1. Introduce `DUUITrait` hierarchy alongside legacy interfaces
2. Make `DUUIEntity<T>` abstract class with legacy interface as default trait set
3. Deprecate legacy interfaces (`DUUIActor`, `DUUIService`, etc.)
4. Migrate concrete classes to trait-based composition
5. Remove legacy interfaces
